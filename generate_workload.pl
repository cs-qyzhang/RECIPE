#!/usr/bin/perl
use v5.10;
use utf8;
use autodie;

print("read file: $ARGV[0]\n");
print("origin file: $ARGV[1]\n");

open(GET, '<', $ARGV[0]);
open(PUT, '<', $ARGV[1]);
open(OUT, '>', 'out.txt');

while (<PUT>) {
    if (/READ/) {
        $_ = <GET>;
        chomp;
        s/INSERT/READ/;
        print(OUT "$_\n");
    } else {
        chomp;
        print(OUT "$_\n");
    }
}