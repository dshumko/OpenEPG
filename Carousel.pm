package DVB::Carousel;

=head1 NAME

DVB::Carousel - Handling of simple DVB carousel database used by ringelspiel.

=head1 SYNOPSIS

Add, delete and list MPEG-2 transport streams chunks in a carousel playout system.

    use DVB::Carousel;

    my $myCarousel = DVB::Carousel->new( 'databasefile');

    # initialize the basic databse table structure
    $myCarousel->initdb();

    # add file to carousel by pid 12 with repetition rate 2000 ms
    $myCarousel->addFile( 12, "nit.ts", 2000);

    # add some binary data to carousel by pid 16 with repetition rate 30 s
    my $data = generateSomeData();
    $myCarousel->addMts( 16, \$data, 30000);

    # delete carousel data with pid 16 
    $myCarousel->deleteData( 16);

=head1 CLASS C<Epg>

=head2 METHODS

=cut

use warnings;
use strict;
use DBI qw(:sql_types);
use Carp;
use Exporter;
use vars qw($VERSION @ISA @EXPORT);

our $VERSION = "0.22";
our @ISA     = qw(Exporter);
our @EXPORT  = qw();

=head3 new( $dbfile )

Class initialization with sqlite3 database filename. 
Open existing or create new sqlite database.

=cut

sub new {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};

    $self->{filename} = shift;
    $self->{dbh}      = DBI->connect( "dbi:SQLite:" . $self->{filename} )
        or return -1;

    $self->{dbh}->do( " PRAGMA synchronous = OFF; 
                        PRAGMA temp_store = MEMORY; 
                        PRAGMA auto_vacuum = NONE; 
                        PRAGMA journal_mode = OFF;
                        PRAGMA cache_size = 4000000;");

    bless( $self, $class );
    return $self;
}

=head3 initdb( )

Initialize database with some basic table structure;
This service can then be played multiple times with different service_id.
Therefore service_id is used when building sections and referencing data in sections.

=cut

sub initdb {
    my $self = shift;
    my $dbh  = $self->{dbh};

    $dbh->do("BEGIN TRANSACTION");

    $dbh->do( "DROP TABLE IF EXISTS carousel");

    $dbh->do( "DROP TABLE IF EXISTS journal");

    $dbh->do(
        "CREATE TABLE carousel ( pid INTEGER,
        interval INTEGER,
        mts BLOB, 
        timestamp DATE,
        PRIMARY KEY( pid))"
    );
    
    $dbh->do(
        "CREATE TABLE journal ( id INTEGER PRIMARY KEY AUTOINCREMENT);"
    );

    # define triggers to trap changes in list of transport streams and update the
    # journal table.
    # This table is used by the playout system to re-read the list of transport 
    # streams to play
    $dbh->do(
        "CREATE TRIGGER journal_carousel_insert 
        BEFORE INSERT ON carousel WHEN (SELECT count(*) FROM carousel WHERE pid=new.pid) = 0
        BEGIN 
        INSERT INTO journal VALUES( NULL);
        END;"
    );

    $dbh->do(
        "CREATE TRIGGER journal_carousel_delete 
        AFTER DELETE ON carousel
        BEGIN 
        INSERT INTO journal VALUES( NULL);
        END;"
    );

    $dbh->do(
        "CREATE TRIGGER journal_carousel_pidchange 
        AFTER UPDATE OF pid ON carousel
        BEGIN 
        INSERT INTO journal VALUES( NULL);
        END;"
    );

    $dbh->do(
        "CREATE TRIGGER journal_carousel_cleaning 
        AFTER INSERT ON carousel
        BEGIN 
        DELETE FROM journal WHERE id != (SELECT id FROM journal ORDER BY id DESC LIMIT 1);
        END;"
    );

    # define triggers that set timestamps on each update
    $dbh->do(
        "CREATE TRIGGER carousel_timestamp_insert 
        AFTER INSERT ON carousel
        BEGIN 
        UPDATE carousel
        SET timestamp = DATETIME('NOW') 
        WHERE pid = new.pid;
        END;"
    );
    $dbh->do(
        "CREATE TRIGGER carousel_timestamp_update 
        AFTER UPDATE ON carousel
        BEGIN 
        UPDATE carousel
        SET timestamp = DATETIME('NOW') 
        WHERE pid = new.pid;
        END;"        
    );

    $dbh->do("COMMIT") or die("error creating database");

    return 1;
}

=item addMts ( $pid, \$mts, $interval)

Add/update MPEG-2 transport stream (MTS) binary data for $pid into carousel. 
The MTS data consists of multiple packets each 188 bytes long.
Return 1 on success.

=cut

sub addMts {
    my $self = shift;
    my ( $pid, $mts, $interval ) = @_;
    my $dbh = $self->{dbh};

    return if ( length($$mts) % 188 ) != 0;
    return if length($$mts) == 0;

    my $insert = $dbh->prepare(
        "INSERT or REPLACE INTO carousel 
        ( pid, interval, mts) VALUES ( $pid, $interval, ?)"
    );

    $insert->bind_param( 1, $$mts, SQL_BLOB );
    return $insert->execute();
}

=item addFile ( $pid, $fileName, $interval)

Same as addMts () except getting MPEG-2 transport stream FROM file.

=cut

sub addFile {
    my $self = shift;
    my ( $pid, $fileName, $interval ) = @_;
    my $dbh = $self->{dbh};
    my $data;

    return if !-e $fileName;

    open( MTSFILE, "<$fileName" ) or return;
    $data = do { local $/; <MTSFILE> };
    close(MTSFILE);

    if ( length($data) > 0 ) {
        return $self->addMts( $pid, \$data, $interval );
    }
    else {
        return;
    }
}

=item deleteMts( $pid)

Remove MTS data from carousel by $pid.
If $pid not defined, delete all.

Return 1 on success.

=cut

sub deleteMts {
    my $self = shift;
    my $pid  = shift;
    my $dbh  = $self->{dbh};

    return $dbh->do( "DELETE FROM carousel WHERE 1"
          . ( defined $pid ? " AND pid='" . $pid . "'" : "" ) );
}

=item listMts( $pid)

List information on MPEG-2 transport stream data in carousel.
$pid is an optional parameter used as selection filter.

Return reference to an array of arrays of MTS consisting of pid, 
repetition interval and timestamp of last update.

=cut

sub listMts {
    my $self = shift;
    my $pid  = shift;
    my $dbh  = $self->{dbh};

    return $dbh->selectall_arrayref( "SELECT pid, interval, strftime('%s',timestamp) AS timestamp FROM carousel WHERE 1"
            . ( defined $pid ? " AND pid=$pid" : "" )
            . ( " ORDER BY pid")); 
}

=item getMts( $pid)

Return reference to array of MPEG-2 transport stream data in carouselfor $pid.
The elements of array are pid, repetition interval, MTS binary data and 
timestamp of last update.

=cut

sub getMts {
    my $self = shift;
    my $pid  = shift;
    my $dbh  = $self->{dbh};

    my $sel = $dbh->selectrow_arrayref( "SELECT pid, interval, mts, strftime('%s',timestamp) FROM carousel WHERE pid=$pid");

    return $sel; 
}
=head1 AUTHOR

Bojan Ramsak, C<< <BojanR@gmx.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dvb-carousel at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DVB-Carousel>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DVB::Carousel

You can also look for information at:

=head1 ACKNOWLEDGEMENTS

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Bojan Ramsak.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;    # End of DVB::Carousel
