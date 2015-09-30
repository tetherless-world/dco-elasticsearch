$num_args = $#ARGV + 1;
if ($num_args != 2) {
    print "\nUsage: perl update_paths.pl dev|prod filename\n";
    exit;
}

print "\nThis script switches between Dev and Prod file paths in a facetview2 browser HTML\n";

$environment=$ARGV[0];
$filename=$ARGV[1];

if ($environment eq 'dev') {
  $cmd1 = "perl -pi -e 's/vivo\\/themes\\/dco\\/js\\/facetview2\\//facetview2\\//g' ".$filename;
  $cmd2 = "perl -pi -e 's/vivo\\/themes\\/dco\\/css\\/browsers.css/browsers.css/g' ".$filename;
} elsif ($environment eq 'prod') {
  $cmd1 = "perl -pi -e 's/facetview2\\//vivo\\/themes\\/dco\\/js\\/facetview2\\//g' ".$filename;
  $cmd2 = "perl -pi -e 's/browsers.css/vivo\\/themes\\/dco\\/css\\/browsers.css/g' ".$filename;
}

system($cmd1);
system($cmd2);
