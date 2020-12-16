#!/usr/bin/perl
use v5.10;
use utf8;
use autodie;
use Excel::Writer::XLSX;

my $workbook = Excel::Writer::XLSX->new( 'ycsb-report.xlsx' );
my $worksheet = $workbook->add_worksheet();
$workbook->add_format(xf_index => 0, font => 'Courier New', align => 'right');

my $row = -1;
my $col = 0;
my $thread = 0;
my $prog = "prog";

chomp(@lines = <>);

foreach (@lines) {
    if (/(.+), workloada, threads (\d+)/) {
        if ($prog ne $1) {
            $row = $row + 2;
            $prog = $1;
            $worksheet->write($row, 0, $prog);
            $col = 1;
        } else {
            $col++;
        }
        $thread = $2;
        $worksheet->write($row-1, $col, $thread);
    } elsif (/Throughput: delete, (\d+(?:\.\d+)?) ,ops/) {
        $worksheet->write($row, $col, "$1");
    }
}

$workbook->close();