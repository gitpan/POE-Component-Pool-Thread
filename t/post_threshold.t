#!/usr/bin/perl

use strict;
use warnings FATAL => "all";
 no warnings "numeric";

use Test::Simple tests => 26;
use POE qw( Component::Pool::Thread );

POE::Component::Pool::Thread->new
( MaxFree       => 5,
  MinFree       => 2,
  MaxThreads    => 8,
  StartThreads  => 3,
  Name          => "ThreadPool",
  EntryPoint    => \&thread_entry_point,
  CallBack      => \&response,
  inline_states => {
    _start  => sub {
        my ($kernel, $session, $heap) = @_[ KERNEL, SESSION, HEAP ];
        my ($thread, @free);

        $kernel->call($session, run => 1) for 1 .. 3;

        $thread = $heap->{thread};
        @free   = grep ${ $_->{semaphore} }, values %$thread;

        ok(scalar keys %$thread == 0);

        $kernel->call($session, run => 0) for 4 .. 20;

        ok @{ $heap->{queue} };

        $kernel->yield(run => "finished");
    },
  }
);

sub thread_entry_point {
    my ($delay) = @_;

    # So we can check
    sleep $delay if int $delay;

    ok 1;

    return $delay;
}

sub response {
    my ($kernel, $heap, $result) = @_[ KERNEL, HEAP, ARG0 ];
    my (@thread, @free);

    @thread  = values %{ $heap->{thread} };
    @free    = grep ${ $_->{semaphore} }, @thread;

    ok @thread <= 8;

    if (@{ $heap->{queue} }) {
        ok ((@free >= 2 && @free <= 5) || (@free == 0 && @thread == 8));
    }
    else {
        # During shut down or quick load drops this happens, but only
        # temporarily.  Eventually the component gets around to GC'ing
        # everything.
        ok @free <= 8;
    }

    if ($result eq "finished") {
        $kernel->yield("shutdown");
    }
}

run POE::Kernel;
