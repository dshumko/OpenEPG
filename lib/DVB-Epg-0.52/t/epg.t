#!perl
use Test::More tests => 21;
use lib "../lib";

my $sampleFile = "sample.db";

BEGIN {
	use_ok( 'DVB::Epg' );
}

my $myeit;


ok( $myeit = DVB::Epg->new( $sampleFile), "new object");

ok( $myeit->initdb(), "create new database");

ok( $myeit->addEit( 18, 1, 8897, 1, 1, 0, 1) == 1, "add eit rule");

### Event handling

my @list = ('News', 'Weather forecast', 'Shrek - the movie', 'Trailer', 'Discovering Linux', 'DIY - eit player', 'Show me this',
            'Extended news', 'History for everybody', 'Chess championship', 'Kitchen cleaning', 'Pinochio', 'Heidi', 'Abba the movie',
            'Holiday in Slovenia', 'Galaxy the easy way', 'What is rsync?');

## Add present/following events without gap

my $start = time()-16*60;
my $stop = time()+21*60;
my $i = 0;
my $short_descriptor;
    $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
    $short_descriptor->{language_code} = 'slv';
    $short_descriptor->{event_name} = 'P' x 110;
    $short_descriptor->{text} = $list[ $i % $#list]." [$start-$stop] ".gmtime($start)."-".gmtime($stop);
ok( $myeit->addEvent( { start=>$start, 
                        stop=>$stop, 
                        uid=>1,
                        descriptors=>[ $short_descriptor]}) == 0, "insert single event");

$start = $stop+1;
$stop = $start+80*60;
    $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
    $short_descriptor->{language_code} = 'slv';
    $short_descriptor->{event_name} = 'F' x 110;
    $short_descriptor->{text} = $list[ $i % $#list]." [$start-$stop] ".gmtime($start)."-".gmtime($stop);
ok( $myeit->addEvent( { start=>$start, 
                        stop=>$stop, 
                        uid=>1}) == 1, "add next event");

ok( $myeit->updateEit( 18) == 1, "use eit rule and update sections (no gap)");
my $mts = $myeit->getEit( 18, 30);
open (FILE, ">bin.ts") or die "napaka";
print( FILE $mts);
close (FILE);

ok( $myeit->deleteEit() == 1, "delete eit rules");
ok( $myeit->deleteEvent( 1, 1) == 1, "delete event by event_id");

## Add present/following events with gap
$myeit->addEit( 18, 5, 8897, 1, 5, 0, 1);

$start = time()-16*60;
$stop = time()-3*60;
$myeit->addEvent( { start=>$start, stop=>$stop, uid=>5});

$start = time()+3*60;
$stop = time()+70*60;
$myeit->addEvent( { start=>$start, stop=>$stop, uid=>5});

ok( $myeit->updateEit( 18) == 1, "use eit rule and update packets (gap)");

$myeit->getEit( 18, 30);

ok( $myeit->deleteEvent() == 3, "delete all events");

ok( $myeit->deleteEit( 18, 5) == 1, "delete eit rule");

### Test schedule
$start = time()-100;
$stop = $start+50; 
for( $i=0; $i<100; $i++) {
    my $short_descriptor;
    $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
    $short_descriptor->{language_code} = 'slv';
    $short_descriptor->{event_name} = "My event name ".('!' x 110);
    $short_descriptor->{text} = $list[ $i % $#list]." [$start-$stop] ".gmtime($start)."-".gmtime($stop);
    $myeit->addEvent( { start=>$start, 
                          stop=>$stop, 
                          uid=>1,
                          descriptors=>[ $short_descriptor]});
    $start = $stop; # +int( rand( 60*10)+1*60);
    $stop = $start+int( rand( 60*100)+10*60); # 10-110 min. 
}

ok( scalar $myeit->listEvent( 1) == 100, "add 100 events without event_id");

for( $i=0; $i<100; $i++) {
    my $short_descriptor;
    $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
    $short_descriptor->{language_code} = 'slv';
    $short_descriptor->{event_name} = "My event name ".('!' x 110);
    $short_descriptor->{text} = $list[ $i % $#list]." [$start-$stop] ".gmtime($start)."-".gmtime($stop);
    $myeit->addEvent( { start=>$start, 
                          stop=>$stop, 
                          uid=>1,
                          id=>($i+399),
                          descriptors=>[ $short_descriptor]});
    $start = $stop+int( rand( 60*10)+5*60);
    $stop = $start+int( rand( 60*100)+10*60); # 10-110 min. 
}

ok( scalar $myeit->listEvent( 1) == 200, "add 100 events with event_id");

ok( $myeit->addEit( 18, 1, 8897, 15, 1, 35, 1) == 1, "add eit rule");

ok( join( "|", @{ ${ $myeit->listEit()}[0]}) eq '18|1|8897|15|1|35|1|', "list eit rules"); 

ok( $myeit->updateEit( 18) == 1, "use eit rules and update packets");

ok( $myeit->updateEit( 18) == 0, "try to update updated packets");

ok( length( $myeit->getEit( 18, 30)) > 1000, "export binary EIT");

ok( $myeit->deleteEvent( undef, undef, time()-60, undef, time()-60) == 199, "delete 199 events seleted by time criteria");

ok( $myeit->deleteEvent( undef, undef, undef, time(), undef, time()) == 1, "delete 1 event from past" );

