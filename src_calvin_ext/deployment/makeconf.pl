#!/usr/bin/perl
#
# This program reads a newline-delimited list of ip addresses from stdin and
# prints a valid config file to stdout whose consecutively-numbered nodes have
# the specified ip addresses as their hostnames.

my $i = 0;
while (<>) {
  chomp;
  print "node$i=0:$i:8:$_:50000\n";
  $i++;
}

