#!/usr/bin/perl

my @hosts;
while (<>) {
  chomp;
  push (@hosts, $_);
  system("ssh $_ \'echo -n\'");
}

foreach (@hosts) {
  system("scp -rp ~/.ssh/known_hosts $_:.ssh/known_hosts");
}
