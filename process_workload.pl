#!/usr/bin/perl

use List::Util qw/shuffle/;

$load_size = 10;
$run_size = 5;

@not_read = (0..$load_size+$run_size-1);

open(RUN_ORG, "<", $ARGV[0]);
open(LOAD, ">", $ARGV[1]);
open(RUN, ">", $ARGV[2]);

while (<RUN_ORG>) {
    chomp;
    if (/READ (\d+)/) {
        $not_read[$1] = 0;
    } else {
        die;
    }
}

print("finish reading $ARGV[0]\n");

@load_data;
@run_data;

for (my $i = 0; $i < $load_size+$run_size; $i++) {
    if ($not_read[$i]) {
        push @run_data, $i;
    } else {
        push @load_data, $i;
    }
}

print("finish split read and non-read\n");

@load_data = shuffle @load_data;
@run_data = shuffle @run_data;

print("finish shuffle before split\n");

while (scalar(@load_data) < $load_size) {
    $_ = shift @run_data;
    push @load_data, $_;
}

@load_data = shuffle @load_data;
@run_data = shuffle @run_data;

print("finish shuffle after\n");

die if scalar(@load_data) != $load_size;
die if scalar(@run_data) != $run_size;

foreach(@load_data) {
    print(LOAD "INSERT $_\n");
}

print("finish writing $ARGV[1]\n");

foreach(@run_data) {
    print(RUN "INSERT $_\n");
}

print("finish writing $ARGV[2]\n");