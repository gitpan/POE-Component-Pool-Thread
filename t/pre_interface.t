#!/usr/bin/perl -l

use strict;
use warnings FATAL => "all";
use Test::Simple tests => 200;

use POE qw( Component::Pool::Thread );

POE::Component::Pool::Thread->new
( MaxFree       => 5,
  MinFree       => 2,
  MaxThreads    => 10,
  StartThreads  => 5,
  Name          => "ThreadPool",
  EntryPoint    => \&thread_entry_point,
  CallBack      => \&response,
  inline_states => {
    _start  => sub {
        $_[KERNEL]->yield(loop => 1),
    },

    loop    => sub {
        my ($kernel, $i) = @_[ KERNEL, ARG0 ];

        $kernel->yield(run => $i);

        if ($i < 100) {
            $kernel->yield(loop => $i + 1);

            # Simulate variable loads
            select undef, undef, undef, rand 0.2;
        }
    },
  }
);

sub thread_entry_point {
    my $point = shift;
    
    # Simulate tasks that take time and block
    select undef, undef, undef, rand 0.5;

    ok $point;

    return $point;
}

sub response {
    my ($kernel, $result) = @_[ KERNEL, ARG0 ];

    ok($result + 100);

    if ($result == 100) {
        $kernel->yield("shutdown");
    }
}

run POE::Kernel;
