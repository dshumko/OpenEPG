#!perl
use Test::More tests => 10;
use lib "../lib";

my $sampleFile = "carousel.db";

BEGIN {
	use_ok( 'DVB::Carousel' );
}

my $mycarousel;

unlink( $sampleFile ) if -e $sampleFile;

ok( $mycarousel = DVB::Carousel->new( $sampleFile), "new object");

ok( $mycarousel->initdb(), "create new database");

my $dummyPes = '?' x 187;
ok( ! defined $mycarousel->addMts( 18, \$dummyPes, 10000), "detect incorrect data insertion");

$dummyPes .= "\x47";
ok( $mycarousel->addMts( 18, \$dummyPes, 10000), "insert data to carousel");

open( FILE, ">sdt.psi");
print( FILE $dummyPes); 
close( FILE);

ok( $mycarousel->addFile( 12, "sdt.psi", 100), "insert file data to carousel");

ok( scalar @{$mycarousel->listMts()} == 2, "list carousel content");

my $meta = $mycarousel->getMts( 18);
ok( $$meta[0] == 18 && $$meta[1] == 10000 && $$meta[2] eq $dummyPes, "get data from carousel");

ok( $mycarousel->deleteMts( 18), "delete carousel data by pid");

ok( $mycarousel->deleteMts() && scalar @{$mycarousel->listMts()} == 0 , "delete all carousel data");

