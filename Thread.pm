package POE::Component::Pool::Thread;
# ----------------------------------------------------------------------------- 
# "THE BEER-WARE LICENSE" (Revision 43) borrowed from FreeBSD's jail.c: 
# <tag@cpan.org> wrote this file.  As long as you retain this notice you 
# can do whatever you want with this stuff. If we meet some day, and you think 
# this stuff is worth it, you can buy me a beer in return.   Scott S. McCoy 
# ----------------------------------------------------------------------------- 

use strict;
use warnings FATAL => "all";
 no warnings 'numeric'; # grep int hack
use threads;
use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use IO::Handle;
use POE qw( Pipe::OneWay Filter::Line Wheel::ReadWrite );
use Fcntl;

*VERSION = \0.01;

use constant DEBUG => 0;

sub new {
    die __PACKAGE__, "->new() requires a balanced list" unless @_ % 2;

    my ($type, %opt) = @_;
    
    $opt{inline_states} ||= {};
    $opt{StartThreads}  ||= 0;

    POE::Session->create    
    ( inline_states => {
        %{ $opt{inline_states} },

        _start => sub {
            my ($kernel, $heap) = @_[ KERNEL, HEAP ];

            $kernel->alias_set($opt{Name}) if $opt{Name};

            my ($pipe_in, $pipe_out) = POE::Pipe::OneWay->new;
            $heap->{pipe_out} = $pipe_out;

            die "Unable to create pipe" 
            unless defined $pipe_in and defined $pipe_out;

            $heap->{wheel} = POE::Wheel::ReadWrite->new
                ( Handle      => $pipe_in,
                  InputEvent  => "-thread_talkback",
                  ErrorEvent  => "-thread_talkerror",
                );

            for (1 .. $opt{StartThreads}) {
                $kernel->yield("-spawn_thread");
            }

            goto $opt{inline_states}{_start} if $opt{inline_states}{_start};
        },

        _stop => sub {
            my ($kernel, $heap) = @_[ KERNEL, HEAP ];

            DEBUG && warn "Joining all threads";
            for my $tid (grep int, keys %$heap) {
                $heap->{$tid}{iqueue}->enqueue("last");
                $heap->{$tid}{thread}->join;
            }

            goto $opt{inline_states}{_stop} if $opt{inline_states}{_stop};
        },

        _default => sub {
            die "_default caught state: ", $_[ARG0];
        },

        -thread_talkerror => sub { die $_[ARG0], $_[ARG2] },

        -thread_talkback => sub {
            my ($kernel, $heap, $input) = @_[ KERNEL, HEAP, ARG0 ];
            my ($tid, $command) = ($input =~ m/(\d+): (\w+)/);

            DEBUG and warn "Recieved: $input";

            # Depending upon the settings of perlvar's, its possible we may get
            # some garbage through here.
            if (defined $command) {
                if ($command eq "cleanup") {
                    $kernel->yield(-execute_cleanup => $tid);
                }
                elsif ($command eq "collect") {
                    $kernel->yield(-collect_garbage => $tid);
                }
            }
        },

        -collect_garbage => sub {
            DEBUG && warn "GC Called, thread exited";
            
            my ($kernel, $heap, $tid) = @_[ KERNEL, HEAP, ARG0 ];

            my $tdsc = delete $heap->{$tid} or return;

            $tdsc->{thread}->join;

            delete $tdsc->{$_} for keys %$tdsc;
        },

        -execute_cleanup => sub {
            my ($kernel, $session, $heap, $tid) = 
                @_[ KERNEL, SESSION, HEAP, ARG0 ];

            DEBUG && warn "GC Called, thread finished task";

            my $queue  = $heap->{queue} ||= [];
            my $rqueue = $heap->{$tid}{rqueue};
            my $iqueue = $heap->{$tid}{iqueue};

            if ($rqueue->pending) {
                if ($opt{Callback}) {
                    DEBUG && warn "Dispatching Callback";
                    $opt{Callback}->( @_[0..ARG0-1], @{$rqueue->dequeue} );
                }
            }

            if (@$queue) {
                my $args = &share([]);
                push @$args, @{ shift @$queue };

                $iqueue->enqueue($args);
            }
        },

        -spawn_thread => sub {
            my ($kernel, $heap) = @_[ KERNEL, HEAP ];
            
            DEBUG && warn "Spawning a new thread";

            my $semaphore   = Thread::Semaphore->new;
            my $iqueue      = Thread::Queue->new;
            my $rqueue      = Thread::Queue->new;
            my $pipe_out    = $heap->{pipe_out};

            my $thread      = threads->create
                ( \&thread_entry_point, 
                  $semaphore, 
                  $iqueue, 
                  $rqueue, 
                  fileno($pipe_out),
                  $opt{EntryPoint} );

            $heap->{$thread->tid} = { 
                semaphore   => $semaphore,
                iqueue      => $iqueue,
                rqueue      => $rqueue,
                thread      => $thread,
                lifespan    => 0, # Not currently used
            };
        },

        run => sub {
            my ($kernel, $heap, @arg) = @_[ KERNEL, HEAP, ARG0 .. $#_ ];

            DEBUG && warn "Assigned a task";

            my @threads = map $heap->{$_}, grep int, keys %$heap;
            my @free    = grep ${ $_->{semaphore} }, @threads;

            if (@free) {
                my $tdsc = shift @free;

                # Trickery so we can pass this through Thread::Queue;
                my $sharg = &share([]);

                # Just to be polite...
                lock $sharg;
                push @$sharg, @arg;

                DEBUG and warn "Enqueueing on ", $tdsc->{thread}->tid;

                $tdsc->{iqueue}->enqueue($sharg);
            }
            else {
                push @{ $heap->{queue} ||= [] }, [ @arg ];
            }

            if (@free < $opt{MinFree}) {
                unless (@threads >= $opt{MaxThreads}) {
                    $kernel->yield("-spawn_thread");
                }
            }
        },

        shutdown => sub {
            my ($kernel, $heap) = @_[ KERNEL, HEAP ];

            $kernel->alias_remove($opt{Name});
            delete $heap->{wheel};
        },
      },
    );
}

sub thread_entry_point {
    my ($semaphore, $iqueue, $rqueue, $pipe_fd, $task) = @_;

    my $pipe = IO::Handle->new_from_fd($pipe_fd, "a") or die $!;

    # XXX Hack
    my $code = $task;

    # Just incase
    local $\ = "\n";

    while (my $action : shared = $iqueue->dequeue) {
        DEBUG and warn threads->self->tid, ": recieved action";
        $semaphore->down;

        lock $action;

        unless (ref $action) {
            if ($action eq "last") {
                $$semaphore = -1;
                last;
            }
        }

        else { 
            my $arg = $action;
            lock $arg;

            # Just incase...
            my $result = &share([]);
            push @$result, $code->(@$arg);

            DEBUG and warn threads->self->tid, ": Enqueuing result: @$result";
            $rqueue->enqueue($result);
        }

        DEBUG and warn threads->self->tid, ": Requesting cleanup";

        $pipe->print( threads->self->tid, ": cleanup" );
        $pipe->flush;

        $semaphore->up;
    }

    $pipe->print( threads->self->tid, ": collect" );
    $pipe->flush;
    DEBUG and warn threads->self->tid, ": Requesting Destruction";
}

1;
