#!/usr/bin/perl

open(READ, "<", $ARGV[0]);
open(INSERT, "<", $ARGV[1]);
open(OUT, ">", $ARGV[2]);

$read_percent = $ARGV[3];
$run_size = 10000000;

for (my $i = 0; $i < $run_size; $i++) {
    if (int(rand(100)) < $read_percent) {
        $_ = <READ>;
    } else {
        $_ = <INSERT>;
    }
    chomp;
    print(OUT "$_\n");
}
