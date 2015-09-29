$num_args = $#ARGV + 1;
if ($num_args != 2) {
    print "\nUsage: switch_dev_prod.pl dev/prod filename\n";
    exit;
}

$environment=$ARGV[0];
$filename=$ARGV[1];

if ($environment eq "prod") {
  $cmd1 = "perl -pi -e 's/facetview2/\\/vivo\\/themes\\/dco\\/js\\/facetview2/g' ".$filename;
  $cmd2 = "perl -pi -e 's/browsers.css/\\/vivo\\/themes\\/dco\\/css\\/browsers.css/g' ".$filename;
} elsif ($environment eq "dev") {
  $cmd1 = "perl -pi -e 's/\\/vivo\\/themes\\/dco\\/js\\/facetview2/facetview2/g' ".$filename;
  $cmd2 = "perl -pi -e 's/\\/vivo\\/themes\\/dco\\/css\\/browsers.css/browsers.css/g' ".$filename;
}

#Update JS paths
system($cmd1);

#Update css path
system($cmd2);
