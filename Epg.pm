package DVB::Epg;

=head1 NAME

DVB::Epg - Generate MPEG-2 transport stream chunk containing DVB Event Information table.

=head1 SYNOPSIS

This module allows generating of DVB EPG service by building EIT p/f and schedule tables.
First some event information must be added to the system. A sqlite database for storage is used.
Based on this event information the library builds the standardized EIT, which can then be
export as a MPEG-2 Transport Stream/chunk for playout. The result of the whole process is an EIT 
inside a MTS.

    use DVB::Epg;

    my $myEpg = DVB::Epg->new( 'eitfile');

    # create empty database
    $myEpg->initdb();

    # add program to EIT for which to generate EPG
    $myEpg->addEit( 18, 9019, 1024, 15, 8, 1);
    
    # add dummy event data to database
    my $event = {};
    $event->{start} = time;
    $event->{stop}  = time+100;
    $event->{uid} = 15;
    $myEpg->addEvent( $event);
    
    # generate EPG tables to database
    $myEpg->updateEit( 18);

    # export EIT as MTS from database
    my $mts = $myEpg->getEit( 18);

The Library can handle multiple services and multiple tables.

=head1 CLASS C<Epg>

=head2 METHODS

=cut

package DVB::EventInformationTable;

package DVB::Epg;

use 5.010;
use strict;
use warnings;
use utf8;
use DBI qw(:sql_types);
use Storable qw(freeze thaw);
use Carp;
use Exporter;
use POSIX qw(ceil);
use Encode;

use vars qw($VERSION @ISA @EXPORT);

our $VERSION = "0.51";
our @ISA     = qw(Exporter);
our @EXPORT  = qw();

=head3 new( $dbfile )

Class initialization with sqlite3 database filename. 
Open existing or create new sqlite database.
if file name is "memory" dtatabase create in memory, without create on hdd

=cut

sub new {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    
    $self->{filename} = shift;
    
    if ($self->{filename} ne "memory") {
        $self->{dbh}      = DBI->connect( "dbi:SQLite:" . $self->{filename} ) or return;
        $self->{dbh}->{sqlite_unicode} = 1;
        $self->{dbh}->do( "PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY; PRAGMA auto_vacuum = NONE; PRAGMA journal_mode = OFF; PRAGMA cache_size = 4000000;" );
    }
    else {
        $self->{dbh} = DBI->connect( "dbi:SQLite:dbname=:memory:") or return;
        $self->{dbh}->do( "PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY; PRAGMA auto_vacuum = NONE; PRAGMA journal_mode = OFF;" );        
    }
    bless( $self, $class );
    return $self;
}

=head3 dbh( )

Return database handle for direct control.

=cut

sub dbh {
    return $_[0]->{dbh};
}

=head3 initdb( )

Initialize database with some basic table structure;

=cut

sub initdb {
    my $self = shift;
    my $dbh  = $self->{dbh};

    $dbh->do("BEGIN TRANSACTION");

    $dbh->do( "DROP TABLE IF EXISTS event");
    $dbh->do( "DROP TABLE IF EXISTS eit");
    $dbh->do( "DROP TABLE IF EXISTS eit_version");
    $dbh->do( "DROP TABLE IF EXISTS section");

    $dbh->do( "CREATE TABLE event ( event_id INTEGER,
                                    uid INTEGER, 
                                    start DATE, 
                                    stop DATE, 
                                    info BLOB, 
                                    timestamp DATE,
                                    PRIMARY KEY( uid, event_id))");

    $dbh->do( "CREATE TABLE eit ( pid INTEGER,                            
                            service_id INTEGER,
                            original_network_id INTEGER,
                            transport_stream_id INTEGER,
                            uid INTEGER,
                            maxsegments INTEGER,
                            actual INTEGER,
                            comment TEXT,
                            PRIMARY KEY( pid, original_network_id, transport_stream_id, service_id))");

    $dbh->do( "CREATE TABLE eit_version ( pid INTEGER,
                                          service_id INTEGER,
                                          table_id INTEGER,
                                          version_number INTEGER,
                                          timestamp DATE,
                                          PRIMARY KEY( pid, service_id, table_id))");

    $dbh->do( "CREATE TABLE section ( pid INTEGER,
                                      table_id INTEGER,
                                      service_id INTEGER,
                                      section_number INTEGER,
                                      dump BLOB, 
                                      PRIMARY KEY( pid, service_id, table_id, section_number))");

    # define triggers that set timestamps on each event when updated
    $dbh->do( "CREATE TRIGGER event_timestamp_insert 
                    AFTER INSERT ON event 
               BEGIN 
                    UPDATE event 
                    SET timestamp = DATETIME('NOW') 
                    WHERE event_id = new.event_id
                    AND uid = new.uid; 
               END;");

    $dbh->do( "CREATE TRIGGER event_timestamp_update 
                    AFTER UPDATE ON event 
               BEGIN 
                    UPDATE event 
                    SET timestamp = DATETIME('NOW') 
                    WHERE event_id = new.event_id 
                    AND uid = new.uid; 
               END;");

    $dbh->do( "CREATE TRIGGER eit_delete 
                    AFTER DELETE ON eit FOR EACH ROW
               BEGIN
                  DELETE FROM eit_version 
                  WHERE eit_version.pid = old.pid
                  AND eit_version.service_id = old.service_id;
                  DELETE FROM section 
                  WHERE section.pid = old.pid
                  AND section.service_id = old.service_id;
               END;");

    return $dbh->do("COMMIT");
}

=head3 addEvent( $event)

Add an $event to event table.
$event must be reference to hash containing at least
fields: $event->{start}, $event->{stop}, $event->{uid}

start, stop MUST be in EPOCH

Optional fields are:
$event->{id}, $event->{running_status}, $event->{free_CA_mode}
and $event->{descriptors}

Return event_key of inserted row.

=cut

sub addEvent {
    my $self    = shift;
    my ($event) = @_;
    my $dbh     = $self->{dbh};

    if ( !exists $event->{uid}
      or !exists $event->{stop}
      or !exists $event->{start}
      or $event->{stop} <= $event->{start}) {
      return;
    }

    $event->{duration} = $event->{stop} - $event->{start};
    $event->{running_status} = exists $event->{running_status} ? $event->{running_status} & 1 : 0;
    $event->{free_CA_mode} = exists $event->{free_CA_mode} ? $event->{free_CA_mode} & 1 : 0;

    # in case when no event_id is defined
    if ( !defined $event->{id}) {

        # find highest event_id currently used
        my @row = $dbh->selectrow_array( "SELECT event_id FROM event WHERE "
                                         . "uid = '$event->{uid}' "
                                         . "ORDER BY event_id DESC LIMIT 1" );

        my $last_event_id;

        # check if query returned result
        if ( $#row == 0 ) {
            $last_event_id = $row[0];
            if ( $last_event_id >= 0xffff ) {

                # check step by step if index from 0 on are in use
                my $num = $dbh->prepare(
                        "SELECT event_id FROM event WHERE "
                      . "uid = '$event->{uid}' "
                      . "ORDER BY event_id" );
                $num->execute();
                my $lastused = -1;
                my $result;
                while ( $result = $num->fetch() ) {
                    if ( ${$result}[0] - $lastused > 1 ) {
                        $last_event_id = $lastused + 1;
                        last;
                    }
                    $lastused = ${$result}[0];
                }
            }
            else {

                # and increment by 1
                ++$last_event_id;
            }
        }
        else {

            # there is no result, no events exist
            $last_event_id = 0;
        }
        $event->{id} = $last_event_id;
    }

    # limit to 16 bit (integer)
    $event->{id} &= 0xffff;

    # prepare the insertation
    my $insert = $dbh->prepare(
		"INSERT or REPLACE INTO event VALUES ( $event->{id}, $event->{uid},
            datetime( $event->{start},'unixepoch'), datetime( $event->{stop},'unixepoch'), ?, NULL)"
    );
    return if !$insert;

    # bind blob and insert event
    $insert->bind_param( 1, freeze($event), SQL_BLOB );
    if ( $insert->execute() ) {
        return $event->{id};
    }
    else {
        return;
    }
}

=head3 listEvent( $uid, $event_id, $start, $stop, $touch)

List events with $uid in cronological order.

$event_id, $start, $stop, $touch are optional parameters.
$event_id is used as selection filter.
$start, $stop are used as interval specification.
If $touch is defined only elements with timestamp newer than 
$touch are returned.

Return array of events.

=cut

sub listEvent {
    my $self = shift;
    my ( $uid, $event_id, $start, $stop, $touch ) = @_;
    my $dbh = $self->{dbh};

    if ( ! defined $uid) {
      return;
    }

    my $sel = $dbh->prepare( "SELECT event_id, uid, strftime('%s',start) AS start, "
          . " strftime('%s',stop) AS time, info, strftime('%s',timestamp) AS timestamp FROM event "
          . " WHERE uid=$uid "
          . ( defined $event_id ? " AND event_id = $event_id" : "" )
          . ( defined $start ? " AND start >= datetime( $start,'unixepoch')" : "")
          . ( defined $stop ? " AND stop <= datetime( $stop,'unixepoch')" : "" )
          . ( defined $touch ? " AND timestamp > datetime( $touch,'unixepoch')" : "")
          . " ORDER BY start" );
    $sel->execute();

    my ( $_event_id, $_uid, $_start, $_stop, $_info, $_timestamp );
    $sel->bind_columns( \( $_event_id, $_uid, $_start, $_stop, $_info, $_timestamp ) );

    my @list;

    while ( $sel->fetch ) {
        my $data = thaw($_info);
        $data->{event_id}   = $_event_id;
        $data->{uid} = $_uid;
        $data->{start}      = $_start;
        $data->{stop}       = $_stop;
        $data->{timestamp}  = $_timestamp;
        push( @list, $data );
    }
    return @list;
}

=head3 deleteEvent( $uid, $event_id, $start_min, $start_max, $stop_min, $stop_max)

Delete events with $uid.

$event_id, $stop_min, $stop_max, $start_min and $start_max are optional parameters.
$uid and $event_id are used as selection filter.

Delete events that have start in between $start_min, $start_max and stop in between 
$stop_min, $stop_max. Use only defined markers.

Return number of deleted events.

=cut

sub deleteEvent {
    my $self = shift;
    my ( $uid, $event_id, $start_min, $start_max, $stop_min, $stop_max) = @_;
    my $dbh = $self->{dbh};
    
    return $dbh->do( "DELETE FROM event WHERE 1"
          . ( defined $uid ? " AND uid=$uid" : "" )
          . ( defined $event_id   ? " AND event_id=$event_id"     : "" )
          . ( defined $start_min  ? " AND start >= datetime( $start_min,'unixepoch')" : "")
          . ( defined $start_max  ? " AND start  < datetime( $start_max,'unixepoch')" : "")
          . ( defined $stop_min   ? " AND stop  >  datetime(  $stop_min,'unixepoch')" : "")
          . ( defined $stop_max   ? " AND stop <= datetime(  $stop_max,'unixepoch')" : "")
    );
}

=head3 addEit( $pid, $service_id, $original_network_id, $transport_stream_id, $uid, $maxsegments, $actual, $comment)

Add eit generator rule.
Maxsegments defines how far in the future the tables should be generated - each segment defines 3 hour period.
All parameters must be defined.

Return 1 on success.

=cut

sub addEit {
    my $self = shift;
    my ( $pid, $service_id, $original_network_id, $transport_stream_id, $uid, $maxsegments, $actual, $comment) = @_;
    my $dbh = $self->{dbh};

    if (  !defined $pid
        or !defined $service_id
        or !defined $original_network_id
        or !defined $transport_stream_id
        or !defined $uid
        or !defined $maxsegments
        or !defined $actual) {
        return;
    };

    $comment = "" if ! defined $comment;
    return $dbh->do( "INSERT or REPLACE INTO eit VALUES ( $pid, $service_id, $original_network_id, $transport_stream_id, $uid, $maxsegments, $actual, '$comment')");

#        $maxsegments, $actual,"."\"\"".")");
}

=head3 listEit( )

List eit generator rules.

Return reference to an array of arrays of rules.

=cut

sub listEit {
    my $self = shift;
    my $dbh = $self->{dbh};

    return $dbh->selectall_arrayref( "SELECT * FROM eit ORDER BY pid, uid"); 
}

=head3 listPid( )

List all destination pid defined in eit generator rules.

Return array of pids.

=cut

sub listPid {
    my $self = shift;
    my $dbh = $self->{dbh};

    my $ref = $dbh->selectcol_arrayref( "SELECT DISTINCT pid FROM eit ORDER BY pid"); 
    return ( defined ($ref) ? @{$ref} : () );
}

=head3 deleteEit( $pid, $service_id, $original_network_id, $transport_stream_id)

Delete eit generator rule.
Parameters are optional.

Return number of deleted rules.

=cut

sub deleteEit {
    my $self = shift;
    my ( $pid, $service_id, $original_network_id, $transport_stream_id) = @_;
    my $dbh = $self->{dbh};

    return $dbh->do( "DELETE FROM eit WHERE 1"
          . ( defined $pid ? " AND pid=$pid" : "" )
          . ( defined $service_id ? " AND service_id=$service_id" : "" )
          . ( defined $original_network_id ? " AND original_network_id=$original_network_id" : "" )
          . ( defined $transport_stream_id ? " AND transport_stream_id=$transport_stream_id" : "" ) );
}

=head3 updateEit( $pid, $forAllPFonly )

Use eit rules for updateing Eit sections of given $pid in database.
$forAllPFonly say that skip create shedule for other TS

Return 1 on success.
Return 0 if sections are already uptodate.
Return undef on error;

=cut

sub updateEit {
    my $self = shift;
    my $pid = shift;
    my $forAllPFonly = shift;
    
    my $dbh  = $self->{dbh};
    my $updated = 0;

    if ( !defined $pid) {
        return;
    }

    my $sel = $dbh->prepare("SELECT * FROM eit WHERE pid=$pid");

    $sel->execute();

    my $ret;
    my $rule;
    while ( $rule = $sel->fetchrow_hashref ) {

        # first calculate present/following
        $ret = $self->updateEitPresent($rule);
        if( ! defined $ret) {
            return;
        };
        $updated |= $ret;

        # and then calculate schedule
        if ( $rule->{maxsegments} > 0 ) {
            $ret = $self->updateEitSchedule( $rule, $forAllPFonly );
            if( ! defined $ret) {
                return;
            };
            $updated |= $ret;
        }
    }
    return $updated;
}

=head3 updateEitPresent( $rule, $forced)

Update eit sections for given $rule.
$rule is reference to hash containing keys:
pid, service_id, original_network_id, transport_stream_id, service_id, maxsegments, actual

Update sections only if there are changes in event table of schedule since last update or 
the $forced flag is set to 1.

Return undef if failed.
Return 0 if sections are already uptodate.
Return 1 after updating sections.

=cut

sub updateEitPresent {
    my $self = shift;
    my $rule = shift;
    my $forced = shift // 0;
    my $dbh  = $self->{dbh};

    # extend the $rule information
    $rule->{table_id} = $rule->{actual} == 1 ? 0x4e : 0x4f;

    my $present_following = new DVB::EventInformationTable($rule);

    # lookup version_number used at last generation of eit and timestamp
    my $select = $dbh->prepare( "SELECT version_number, strftime('%s',timestamp) FROM eit_version "
            ." WHERE pid=$rule->{pid} AND table_id=$rule->{table_id} AND service_id=$rule->{service_id}" );

    $select->execute();
    my ( $last_version_number, $last_update_timestamp ) = $select->fetchrow_array();

    if( $forced) {
        $last_update_timestamp = 0;
    }

    # if lookup wasn't succesfull we need to update the eit anyway
    if ( !defined $last_version_number ) {
        $last_update_timestamp = 0;
        $last_version_number   = 0;
    }


    # always use this time in queries
    my $current_time = time();

    # find last started event
    $select = $dbh->prepare( "SELECT event_id, strftime('%s',start) AS start, strftime('%s',stop) AS stop, "
                . " info, strftime('%s',timestamp) AS timestamp FROM event "
                . " WHERE uid=$rule->{uid} AND start <= datetime( $current_time,'unixepoch') ORDER BY start DESC LIMIT 1" );
    $select->execute();

    my $last_started_event = $select->fetchrow_hashref;

    # find following event
    $select = $dbh->prepare( "SELECT event_id, strftime('%s',start) AS start, strftime('%s',stop) AS stop, "
                . " info, strftime('%s',timestamp) AS timestamp FROM event "
                . " WHERE uid=$rule->{uid} AND start > datetime( $current_time,'unixepoch') ORDER BY start LIMIT 1" );
    $select->execute();

    my $following_event = $select->fetchrow_hashref;

    my $buildEit = 0;

    # check if we need an update
    # is the last started event still lasting
    if ( defined $last_started_event && $last_started_event->{stop} > $current_time ) {

        # was the start already published or is there a change in the event data
        if (
            $last_started_event->{start} > $last_update_timestamp
            ||    # present event started after last update of eit
            $last_started_event->{timestamp} > $last_update_timestamp
            ||    # present event was modified since last update of eit
            defined $following_event
            && $following_event->{timestamp} > $last_update_timestamp
          )       # following event was modified since last update of eit
        {
            $buildEit = 1;
        }
    }
    else {

        # last event is over - there is a gap now

        # was the end of the last event published or is there a change in event data of following event
        if ( defined $last_started_event && $last_started_event->{stop} > $last_update_timestamp
            ||    # end of last started event was not pulished
            defined $following_event && $following_event->{timestamp} > $last_update_timestamp
          )       # followig event was modified
        {
            $buildEit = 1;
        }
    }

    return 0 if !$buildEit;

    my $pevent;

    # if there is a current event add it to table
    # or add an empty section
    if ( defined $last_started_event && $last_started_event->{stop} > $current_time ) {
        $pevent = _unfreezeEvent( $last_started_event );
        $pevent->{running_status} = 4;
    }
    $present_following->add2Section( 0, $pevent );

    # if there is a following event add it to table
    my $fevent;
    if ( defined $following_event ) {
        $fevent = _unfreezeEvent( $following_event );
        $fevent->{running_status} = ( $following_event->{start} - $current_time ) < 20 ? 2 : 1;
    }
    $present_following->add2Section( 1, $fevent );

    #
    # Add this to playout and update version
    ++$last_version_number;

    # Remove all section of this table
    return 
      if !$dbh->do( "DELETE FROM section WHERE pid=$rule->{pid} AND service_id=$rule->{service_id} AND table_id=$rule->{table_id}" );

    my $insert = $dbh->prepare( "INSERT INTO section VALUES ( $rule->{pid}, $rule->{table_id}, $rule->{service_id}, ?, ?)");
    return if !$insert;

    my $sections = $present_following->getSections($last_version_number);

    foreach my $section_number ( keys %$sections ) {
        $insert->bind_param( 1, $section_number );
        $insert->bind_param( 2, $sections->{$section_number}, SQL_BLOB );
        $insert->execute();
    }
    return $dbh->do( "INSERT OR REPLACE INTO eit_version VALUES ($rule->{pid}, $rule->{service_id}, "
            . "$rule->{table_id}, $last_version_number, datetime( $current_time,'unixepoch'))"
    );
}

=head3 updateEitSchedule( $rule, $forAllPFonly )

Update eit playout packet for given $rule.
$rule is reference to hash containing keys:
pid, service_id, original_network_id, transport_stream_id, service_id, maxsegments, actual
$forAllPFonly say that skip create shedule for other TS

=cut

sub updateEitSchedule {
    my $self = shift;
    my $rule = shift; 
    my $forAllPFonly = shift;
    
    my $dbh  = $self->{dbh};

    my $num_subtable = int( ( $rule->{maxsegments} - 1 ) / 32 );

    # always use this time in queries
    my $current_time = time();
    my $last_midnight = int( $current_time / ( 24 * 60 * 60 ) ) * 24 * 60 * 60;
    
    # it's magic :) 7200 it's one hour ago in seconds (1*60*60) for shedule show completed program
    my $current_time_shift = $current_time - 3600;
    if ($last_midnight == int( $current_time_shift / ( 24 * 60 * 60 ) ) * 24 * 60 * 60) {
        $current_time = $current_time_shift;
    }
    
    # iterate over all subtables
    my $subtable_count = 0;
    
    while ( $subtable_count <= $num_subtable ) {
        
        if ($forAllPFonly == 1) {
            if ($rule->{actual} != 1) { next; }
        }
        
        # extend the $rule information
        $rule->{table_id} =
          ( $rule->{actual} == 1 ? 0x50 : 0x60 ) + $subtable_count;
        
        my $schedule = new DVB::EventInformationTable($rule);
        
        # lookup version_number used at last generation of eit and timestamp
        my $select = $dbh->prepare(
            "SELECT version_number, strftime('%s',timestamp) FROM eit_version 
                WHERE pid=$rule->{pid} AND table_id=$rule->{table_id} AND service_id=$rule->{service_id}"
        );
        $select->execute();
        my ( $last_version_number, $last_update_timestamp ) =
          $select->fetchrow_array();
        
        # if lookup wasn't succesfull we need to update the eit anyway
        if ( !defined $last_version_number ) {
            $last_update_timestamp = 0;
            $last_version_number   = 0;
        }

        # first segment number in this subtable
        my $first_segment = $subtable_count * 32;

        # start of subtable interval
        my $subtable_start = $last_midnight + $first_segment * 3 * 60 * 60;

        # last segment in this subtable (actually it is the next of the last)
        my $last_segment =
            $rule->{maxsegments} >= $first_segment + 32
          ? $first_segment + 32
          : $rule->{maxsegments};

        # end of subtable interval and maxsegments
        my $subtable_stop = $last_midnight + $last_segment * 3 * 60 * 60;

        # find last modification time of events in this subtable
        $select = $dbh->prepare( "SELECT strftime('%s',timestamp) AS timestamp FROM event "
                . "WHERE uid=$rule->{uid} "
                . "AND start >= datetime( $subtable_start,'unixepoch') "
                . "AND start < datetime( $subtable_stop,'unixepoch') "
                . "ORDER BY timestamp DESC LIMIT 1" );
        $select->execute();
        my ($last_event_modification) = $select->fetchrow_array() || 0;

        # has there any event stopped since last update
        # if yes this event can be removed from schedule
        my ($n) = $dbh->selectrow_array( "SELECT count(*) FROM event "
                . "WHERE uid=$rule->{uid} "
                . "AND stop > datetime( $last_update_timestamp,'unixepoch') "
                . "AND stop < datetime( $current_time,'unixepoch')" );

        # skip this subtable if there is no need for updating
        next if $last_update_timestamp >= $last_midnight
                and $last_event_modification <= $last_update_timestamp
                and $n == 0;

        # iterate over each segment
        my $segment_count = $first_segment;
        while ( $segment_count < $last_segment ) {

            # segment start is in future
            if ( $last_midnight + $segment_count * 3 * 60 * 60 >= $current_time ) {
                $select = $dbh->prepare( "SELECT event_id, strftime('%s',start) AS start, "
                        . "strftime('%s',stop) AS stop, info, "
                        . "strftime('%s',timestamp) AS timestamp FROM event "
                        . "WHERE uid=$rule->{uid} "
                        . "AND start >= datetime( " . ( $last_midnight + $segment_count * 3 * 60 * 60 ) . ",'unixepoch') "
                        . "AND start < datetime( " . ( $last_midnight + ( $segment_count + 1 ) * 3 * 60 * 60 ) . ",'unixepoch') "
                        . "ORDER BY start" );
                $select->execute();

                my $event;
                while ( $event = $select->fetchrow_hashref ) {
                    my $ue = _unfreezeEvent($event);
                    $ue->{running_status} = 1;
                    $schedule->add2Segment( $segment_count, $ue );    
                    # TODO what if all sections are in use
                }
            }

            # segment stop is in past
            elsif ( $last_midnight + ( $segment_count + 1 ) * 3 * 60 * 60 - 1 < $current_time ) {
                # add empty segment
                $schedule->add2Section( ( $segment_count % 32 ) * 8 );
            }

            # segment start is in past but segment end is in future
            else {
                $select = $dbh->prepare( "SELECT event_id, strftime('%s',start) AS start, strftime('%s',stop) AS stop, "
                        . "info, strftime('%s',timestamp) AS timestamp FROM event "
                        . "WHERE uid=$rule->{uid} "
                        . "AND stop >= datetime( $current_time,'unixepoch') "
                        . "AND start < datetime( " . ( $last_midnight + ( $segment_count + 1 ) * 3 * 60 * 60 ) . ",'unixepoch') "
                        . "ORDER BY start");
                $select->execute();

                my $event;
                while ( $event = $select->fetchrow_hashref ) {
                    my $ue = _unfreezeEvent($event);
                    $ue->{running_status} = $event->{start} < $current_time ? 4 : 1;
                    $schedule->add2Segment( $segment_count, $ue );
                    # TODO what if all sections are in use
                }
            }
            ++$segment_count;
        }

        # Add subtable to playout and update version
        ++$last_version_number;

        # Remove all section of this table
        return if !$dbh->do( "DELETE FROM section "
                . "WHERE pid=$rule->{pid} "
                . "AND service_id=$rule->{service_id} "
                . "AND table_id=$rule->{table_id}" );

        my $insert = $dbh->prepare( "INSERT INTO section VALUES ( $rule->{pid}, $rule->{table_id}, $rule->{service_id}, ?, ?)" );
        return  if !$insert;

        my $sections = $schedule->getSections($last_version_number);

        foreach my $section_number ( keys %$sections ) {
            $insert->bind_param( 1, $section_number );
            $insert->bind_param( 2, $sections->{$section_number}, SQL_BLOB );
            $insert->execute();
        }

        return if !$dbh->do( "INSERT OR REPLACE INTO eit_version VALUES ( $rule->{pid}, $rule->{service_id}, $rule->{table_id}, $last_version_number, datetime( $current_time,'unixepoch'))");
    }
    continue {
        ++$subtable_count; 
    }
    return 0;
}

=head3 getEit( $pid, $timeFrame )

Build final EIT from all sections in table for given $pid and $timeFrame.

Return the complete TS chunk to be played within the timeframe. 
Default timeframe should be 60s.
Return undef on error.

=cut

sub getEit {
    my $self = shift;
    my $pid  = shift;
    my $timeFrame = shift;      # this is the time frame for which we are building the fragment of the TS
    
    my $dbh  = $self->{dbh};

    if ( !defined $pid) {
        return;
    }
    if( !defined $timeFrame or $timeFrame < 10) {
        return;
    }

    # fetch all sections from database
    my $sel = $dbh->prepare( "SELECT table_id, service_id, section_number, dump FROM section WHERE pid=$pid" );
    $sel->execute();

    my ( $_table_id, $_service_id, $_section_number, $_dump );
    $sel->bind_columns( \( $_table_id, $_service_id, $_section_number, $_dump ) );

    my %pfSections = ( present => { packetCount => 0, mts => ''},
                     following => { packetCount => 0, mts => ''});
    my $pfFrequency = ceil($timeFrame / 1.7);    # DON'T CHANGE THIS, IT IS THE BASIC CYCLE
    # the repetition period must be at least 2s by 
    
    my @otherSections;
    my $allPacketCount = 0;

    # convert section into MPEG transport stream package and store in hash with
    # some basic information for building up the final MTS
    # the sections are grouped by present, following and other
    while ( $sel->fetch ) {
        my $section;
        my $mts = _packetize( $pid, $_dump );
        $section->{mts}            = $mts;
        $section->{size}           = length($mts) / 188;
        $section->{frequency}      = $self->getSectionFrequency( $_table_id, $_section_number, $timeFrame );
        $section->{table_id}       = $_table_id;
        $section->{service_id}     = $_service_id;
        $section->{section_number} = $_section_number;

        # p/f table have a higher repetition rate (every 2s) and therefore are grouped separate
        if( $_table_id == 0x4e) {
            $section->{frequency} = $pfFrequency;
            if( $_section_number == 0) {
                $pfSections{present}{packetCount} += $section->{size};
                $pfSections{present}{mts} .= $section->{mts};
            }
            else {
                $pfSections{following}{packetCount} += $section->{size};
                $pfSections{following}{mts} .= $section->{mts};
            }
        }
        else {
            push( @otherSections, $section);
        }
        $allPacketCount += $section->{frequency} * $section->{size};
    }

    # calculate available space for other sections than present following
    my $nettoSpace = $allPacketCount - $pfFrequency * ( $pfSections{present}{packetCount} + $pfSections{following}{packetCount});
    # we are going to put the sections as following
    # PRESENT other FOLLOWING other PRESENT other FOLLOWING other ....
    # therefore we have 2 x $pfFrequency gaps to fill up with other sections
    my $interPfGap = $nettoSpace / (2*$pfFrequency);
    # it is intentionally decimal number, if there are a small number of sections

    # based on nettoSpace we can calculate the
    # specifical spacing between each repetition of a section
    foreach my $section ( @otherSections) {
        $section->{spacing} = int( $nettoSpace / $section->{frequency} + .5 ) - $section->{size} - 1;

        # this will be used to call down, when the next repetition should occur
        $section->{nextApply} = 0;

        # has the section already been played
        $section->{played} = 0;
    }
    
#    printf( " all: %4i netto: %4i gap: %4i rest: %4i\n", $allPacketCount, $nettoSpace, $interPfGap, $nettoSpace-$pfFrequency*$interPfGap);

    # let's build the stream
    my $pfCount = 2*$pfFrequency;
    my $finalMts       = '';
    my $gapSpace = 0;
    while ( $pfCount > 0 ) {

        # put alternating present and following mts in the stream
        if( $pfCount % 2 == 0) {
            $finalMts .= $pfSections{present}{mts};
            $allPacketCount -= $pfSections{present}{packetCount};
        }
        else {
            $finalMts .= $pfSections{following}{mts};
            $allPacketCount -= $pfSections{following}{packetCount};
        }

        $pfCount -= 1;

        # now fill up the gap with other section
        $gapSpace += $interPfGap;

        # at last iteration we need to put all remaining packets in the stream
        $gapSpace = $allPacketCount if $pfCount == 0;

        my $sectionCount = 0; 

        while( $gapSpace > 0 && $allPacketCount > 0) {
            # sort sections by number when it has to apply, frequency and size
            @otherSections = sort {
                     $a->{nextApply} <=> $b->{nextApply}
                  || $b->{frequency} <=> $a->{frequency}
#                  || int(rand(3))-1
                } @otherSections;
            
            my $j = 0;
            
            $sectionCount += 1;
            my $numInsertedPackets = $otherSections[$j]->{size};

            $gapSpace -= $numInsertedPackets;

            # add sections to output
            $finalMts .= $otherSections[$j]->{mts};

            $otherSections[$j]->{frequency} -= 1;
            $otherSections[$j]->{nextApply}  = $otherSections[$j]->{spacing};  
            $otherSections[$j]->{played}     = 1;

            $allPacketCount -= $numInsertedPackets;

#            printf( " j: %3i size: %2i gapspace: %3i pfcount: %2i all: %3i\n", $j, $otherSections[$j]->{size}, $gapSpace, $pfCount, $allPacketCount);

            # if all repetitions have been done, remove section from pool
            if ( $otherSections[0]->{frequency} == 0 ) {
                splice( @otherSections, 0, 1); # remove finished sections
            }

            $j = 0;
            # correct counters for all sections that have been already played
            while ( $j <= $#otherSections ) {
                $otherSections[$j]->{nextApply} -= $numInsertedPackets if $otherSections[$j]->{played};
                $j += 1;
            }

        }
    }

    # correct continuity counter
    my $continuity_counter = 0;
    for ( my $j = 3 ; $j < length($finalMts) ; $j += 188 ) {
        substr( $finalMts, $j, 1, chr( 0b00010000 | ( $continuity_counter & 0x0f ) ) );
        $continuity_counter += 1;
    }

    return $finalMts;
}

=head3 getSectionFrequency( $table_id, $section_number, $timeFrame)

Make lookup by $table_id and $section_number and return how often this section
has to be repeated in the given interval. Default interval ($timeFrame) is 60 seconds.

=cut

sub getSectionFrequency {
    my $self           = shift;
    my $table_id       = shift;
    my $section_number = shift;
    my $timeFrame      = shift;
    $timeFrame = 60 if !defined $timeFrame;

  # according to some scandinavian and australian specification we use following
  # repetition rate:
  # EITp/f actual              - every <2s
  # EITp/f other               - every <10s
  # EITsched actual 1 day      - every 10s
  # EITsched actual other days - every 30s
  # EITsched other 1 day       - every 30s
  # EITsched other other days  - every 30s
  # THE FREQUENCY FOR PRESENT/FOLLOWING TABLE 0X4E IS DEFINED IN THE CALLING SUBROUTINE
    return ceil($timeFrame / 8) if $table_id == 0x4f;
    return ceil($timeFrame / 10) if ( $table_id == 0x50) and ( $section_number < (1 * 24 / 3 )); # days * 24 / 3
    return ceil($timeFrame / 30);
}

=head3 getLastError( )

Return last db operation error.

=cut

sub getLastError {
    my $self = shift;
    my $dbh  = $self->{dbh};

    return $dbh->errstr;
}

=head3 _packetize( $pid, $section)

Generate MPEG transport stream for defined $pid and $section in database.
Continuity counter starts at 0;
Return MTS.

=cut

sub _packetize {
    my $pid                 = shift;
    my $data                = "\x00" . shift;   # add the pointer field at the beginning
    my $continuity_counter  = 0;
    my $packet_payload_size = 188 - 4;
    my $data_len = length($data); 
      # 'pointer_field' is only in the packet, carrying first byte of this section.
      # Therefore this packet has 'payload_unit_start_indicator' equal '1'.
      # All other packets don't have a 'pointer_filed' and therefore
      # 'payload_unit_start_indicator' is cleared
      #
    my $offs = 0;
    my $mts  = "";

    while ( my $payload = substr( $data, $offs, $packet_payload_size ) ) {

        # Add stuffing byte to payload
        my $stuffing_bytes = $packet_payload_size - length($payload);
        while ( $stuffing_bytes-- ) { $payload .= "\xff"; }

        # Header + Payload:
        my $payload_unit_start_indicator = $offs == 0 ? 0b0100 << 12 : 0;    # payload_unit_start_indicator
        my $packet = pack( "CnC",
                    0x47,
                    $pid | $payload_unit_start_indicator,
                    0b00010000 | ( $continuity_counter & 0x0f ) ) . $payload;
        $mts .= $packet;
        $offs += $packet_payload_size;
        ++$continuity_counter;
        last if $offs > $data_len - 1;
    }
    return $mts;
}

=head3 _unfreezeEvent( $event)

$event is a reference to hash containing elements of a row in event table.
Thaw the info field and update all other keys from field values. 

Return reference to updated info hash.

=cut

sub _unfreezeEvent {
    my $row = shift;

    return if !$row;

    my $event = thaw( $row->{info} );
    $event->{event_id} = $row->{event_id};
    $event->{start}    = $row->{start};
    $event->{stop}     = $row->{stop};
    $event->{duration} = $row->{stop} - $row->{start};
    return $event;
}

=head1 CLASS C<EventInformationTable>

=head2 METHODS

=cut

package DVB::EventInformationTable;

use strict;
use warnings;
use Digest::CRC qw(crc);
use Carp;
use Exporter;
use vars qw(@ISA @EXPORT);

our @ISA    = qw(Exporter);
our @EXPORT = qw();

=head3 new( $rule )

EIT subtable initialization with information taken from $rule.

=cut

sub new {
    my $this  = shift;
    my $rule  = shift or return;
    my $class = ref($this) || $this;
    my $self  = {};

    bless( $self, $class );

    $self->{table}                       = 'EIT';
    $self->{table_id}                    = $rule->{table_id};
    $self->{pid}                         = $rule->{pid};
    $self->{service_id}                  = $rule->{service_id};
    $self->{last_section_number}         = undef;
    $self->{transport_stream_id}         = $rule->{transport_stream_id};
    $self->{original_network_id}         = $rule->{original_network_id};
    $self->{uid}                         = $rule->{uid};
    $self->{segment_last_section_number} = undef;

    if ( $rule->{maxsegments} == 0 ) {

        # there is just present/following
        $self->{last_table_id} = $self->{table_id};
    }
    else {

        # we have more subtables
        my $st = int( $rule->{maxsegments} / 32 );
        if ( $rule->{actual} == 1 ) {
            $self->{last_table_id} = 0x50 + $st;
        }
        else {
            $self->{last_table_id} = 0x60 + $st;
        }
    }
    $self->{sections} = [];

    return $self;
}

=head3 add2Segment( $segment_number, $event)

Add $event to segment with number $segment_number.
$event is reference to hash containin event data.

Return 1 on success.
Return undef on error.

=cut

sub add2Segment {
    my $self           = shift;
    my $segment_number = shift;
    my $event          = shift;

    if ( !defined $segment_number or !defined $event ) {
        return;
    }

    my $target_section         = ( $segment_number % 32 ) * 8;
    my $largest_target_section = $target_section + 8;
    my $size;

    while ( ( ( $size = $self->add2Section( $target_section, $event ) ) == -1 ) and $target_section < $largest_target_section ) {
        ++$target_section;
    }
    return $size;
}

=head3 add2Section ( $section_number, $event)

Add $event to section with number $section_number.
$event is reference to hash containin event data.

Return binary $size of all events in section (always < 4078) 
or negativ if section is full, undef on error.

=cut

sub add2Section {
    my $self           = shift;
    my $section_number = shift;
    my $event          = shift;

    return if !defined $section_number;

    my $section_size = length( $self->{sections}[$section_number] // "" );

    # add empty event
    if ( !defined $event ) {
        $self->{sections}[$section_number] .= "";
        return $section_size;
    }

    my $alldescriptors = "";

    # iterate over event descriptors
    foreach ( @{ $event->{descriptors} } ) {
        $alldescriptors .= _getDescriptorBin($_);
    }

    my $descriptor_loop_length = length($alldescriptors);

    # build binary presentation
    my $struct = pack( 'na5a3na*',
                $event->{event_id},
                _epoch2mjd( $event->{start} ),
                _int2bcd( $event->{duration} ),
                ( ( ( ( $event->{running_status} & 0x07 ) << 1 ) + ( $event->{free_CA_mode} & 0x01 )) << 12) + $descriptor_loop_length,
                $alldescriptors
    );

    my $struct_size = length($struct);

    # add to section if enough space left
    if ( $section_size + $struct_size < 4078 ) {
        $self->{sections}[$section_number] .= $struct;
        return $section_size + $struct_size;
    }
    else {

        return -1;
    }
}

=head3 getSections ()

Return reference to hash of sections with section_number as key and section as value.

=cut

sub getSections {
    my $self           = shift;
    my $version_number = shift // 0;
    my $sections       = {};

    my $last_section_number = $#{ $self->{sections} };
    my $num_segments        = int( $last_section_number / 8 );

    my $current_segment = 0;

    # iterate over segments
    while ( $current_segment <= $num_segments ) {

        # find last used section in this segment
        my $i = 7;
        while ( $i >= 0 and !defined $self->{sections}[ $current_segment * 8 + $i ] ) {
            --$i;
        }
        my $segment_last_section_number = $i + $current_segment * 8;

        # iterate over sections in this segment and add them to final hash
        my $current_section = $current_segment * 8;
        while ( $current_section <= $segment_last_section_number ) {
            my $section_length = length( $self->{sections}[$current_section] ) + 15;
            my $struct = pack( 'CnnCCCnnCCa*',
                        $self->{table_id},
                        ( (0x01) << 15 ) + 0x7000 + $section_length,    # section_syntax_indicator is always 1
                        $self->{service_id}, 0xc0 + ( $version_number & 0x1f << 1 ) + 0x01,               # current_next indicator MUST be always 1
                        $current_section,
                        $last_section_number,
                        $self->{transport_stream_id},
                        $self->{original_network_id},
                        $segment_last_section_number,
                        $self->{last_table_id},
                        $self->{sections}[$current_section]
            );
            my $crc = crc( $struct, 32, 0xffffffff, 0x00000000, 0, 0x04C11DB7, 0, 0);

            # add the binary to result
            $sections->{$current_section} = $struct . pack( "N", $crc );
            ++$current_section;
        }
        ++$current_segment;
    }
    return $sections;
}

=head3 _getDescriptorBin ( $descriptor)

Return binary representation of $descriptor.

=cut

sub _getDescriptorBin {
    my $descriptor = shift;
    my $struct;

    if ( $descriptor->{descriptor_tag} == 0x4d ) {

        # short_event_descriptor
        my $descriptor_tag = 0x4d;
        my $descriptor_length;
        my $language_code   = _getByteString( $descriptor->{language_code} // 'rus');
        my $codepage_prefix = _getByteString( $descriptor->{codepage_prefix});
        my $raw_event_name  = $descriptor->{event_name} // '';
        my $raw_text        = $descriptor->{text} // '';
        
        my $codepage_prefix_length = length( $codepage_prefix );

        my $event_name = "";
        if ( $raw_event_name ne "") {
            $event_name = $codepage_prefix . substr( _getByteString($raw_event_name), 0, 255 - 5 - $codepage_prefix_length );
        }
        my $event_name_length = length( $event_name );

        my $text = "";
        if ( $raw_text ne "") {
            $text = $codepage_prefix . substr( _getByteString($raw_text), 0, 255 - 5 - $event_name_length - $codepage_prefix_length );
        }
        my $text_length = length( $text );

        $descriptor_length = $event_name_length + $text_length + 5;
        $struct            = pack( "CCa3Ca*Ca*",
            $descriptor_tag, $descriptor_length, $language_code,
            $event_name_length, $event_name, $text_length, $text );

    }
    elsif ( $descriptor->{descriptor_tag} == 0x55 ) {
        
        # parental_rating_descriptor
        my $descriptor_tag = 0x55;
        my $descriptor_length;

        my $substruct = '';
        foreach ( @{ $descriptor->{list} } ) {
            my $country_code = _getByteString( $_->{country_code} // 'RUS');
            my $rating       = $_->{rating} // 0;
            if (defined $rating) {
                if ($rating > 0 ) {
                    $substruct .= pack( "a3C", $country_code, $rating );
                }
            }
        }
        $descriptor_length = length($substruct);
        $struct = pack( "CCa*", $descriptor_tag, $descriptor_length, $substruct );
    }
    elsif ( $descriptor->{descriptor_tag} == 0x54 ) {

        # dvb_genre_descriptor
        my $descriptor_tag = 0x54;
        my $descriptor_length;

        my $substruct = '';
        foreach ( @{ $descriptor->{list} } ) {
            my $genre       = $_->{dvb};
            if (defined $genre) {
                $substruct .= pack( "CC", $genre, 0x0 );
            }
        }
        $descriptor_length = length($substruct);
        $struct = pack( "CCa*", $descriptor_tag, $descriptor_length, $substruct );
    }    
    elsif ( $descriptor->{descriptor_tag} == 0x4e ) {

        # extended_event_descriptor
        $struct = _getExtendedEventDescriptorBin( $descriptor );
    }
    else {
        return "";
    }

    return $struct;
}

=head3 _getByteString ( $string)

Convert $string containing only byte characters.
This is for avoiding any problems with UTF8.
Those string must be converted before entering data into database.

Return converted string.

=cut

sub _getByteString {
    my $string = shift;
    return "" if ! $string;

    if ( utf8::is_utf8($string) ) {
        $string = Encode::encode("utf8", $string);
    }

    return pack( "C*", unpack( "U*", $string ) );
}

=head3 _getExtendedEventDescriptorBin( $descriptor)

Return 1 or many Extended Event Descriptors

=cut

sub _getExtendedEventDescriptorBin {
    my $descriptor = shift;
    my $struct     = "";

    # skip if nothing to do
    return '' if !exists $descriptor->{text} || !defined $descriptor->{text} || $descriptor->{text} eq "";

    my $fulltext         = _getByteString( $descriptor->{text} );
    my $full_text_length = length($fulltext);

    # the limit for this is 16 x 255 by numbers of extended event descriptors
    # also is a limit the max. section size 4096
    # let's say the max is 1024
    if ( $full_text_length > 1010 ) {
        my $firstPart = substr( $fulltext, 1010 );    # shorten text
        $fulltext         = $firstPart;
        $full_text_length = length($fulltext);
    }

    # split up the text into multiple Extended Event Descriptors
    my $maxTextLength          = 255 - 6;
    my $last_descriptor_number = int( $full_text_length / $maxTextLength );

    my $descriptor_tag         = 0x4e;
    my $language_code          = _getByteString( $descriptor->{language_code} // 'rus');
    my $codepage_prefix        = _getByteString( $descriptor->{codepage_prefix});
    my $codepage_prefix_length = length($codepage_prefix);
    my $descriptor_length;
    my $length_of_items = 0;
    my $text;
    my $text_length;
    my $descriptor_number = 0;

    while ( $descriptor_number <= $last_descriptor_number ) {
        $text = $codepage_prefix . substr( $fulltext, 0, $maxTextLength - $codepage_prefix_length, '' );
        $text_length       = length($text);
        $descriptor_length = $text_length + 6;
        $struct .= pack( "CCCa3CCa*", 
                    $descriptor_tag,
                    $descriptor_length,
                    $descriptor_number << 4 | $last_descriptor_number,
                    $language_code,
                    $length_of_items,
                    $text_length,
                    $text
        );
        ++$descriptor_number;
    }
    return $struct;
}

=head3 _int2bcd( $time)

Convert integer $time in seconds into 24 bit time BCD format (hour:minute:seconds).

=cut

sub _int2bcd {
    my $time   = shift;
    my $hour   = int( $time / ( 60 * 60 ) );
    my $min    = int( $time / 60 ) % 60;
    my $sec    = $time % 60;
    my $struct = pack( 'CCC',
        int( $hour / 10 ) * 6 + $hour,
        int( $min / 10 ) * 6 + $min,
        int( $sec / 10 ) * 6 + $sec );
    return $struct;
}

=head3 _bcd2int( $bcd)

Convert time in 24 bit BCD format (hour:minute:seconds) in seconds from midnight;

=cut

sub _bcd2int {
    my $bcd = shift;
    my ( $hour, $min, $sec ) = unpack( 'H2H2H2', $bcd );
    my $int = ( $hour * 60 + $min ) * 60 + $sec;
    return $int;
}

=head3 _epoch2mjd( $time)

Convert epoch $time into 40 bit Modified Julian Date and time BCD format.

=cut

sub _epoch2mjd {
    my $time = shift;
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday ) = gmtime($time);
    ++$mon;

    my $l = $mon == 1 || $mon == 2 ? 1 : 0;
    my $MJD = 14956 + $mday + int( ( $year - $l ) * 365.25 ) + int( ( $mon + 1 + $l * 12 ) * 30.6001 );
    my $struct = pack( 'na*', $MJD, _int2bcd( $time % ( 60 * 60 * 24 ) ) );
    return $struct;
}

=head3 _mjd2epoch( $time)

Convert 40 bit Modified Julian Date and time BCD format into epoch.

=cut

sub _mjd2epoch {
    my $combined = shift;
    my ( $mjd, $bcd ) = unpack( 'na3', $combined );

    my ( $y, $m );
    $y = int( ( $mjd - 15078.2 ) / 365.25 );
    $m = int( ( $mjd - 14956 - int( $y * 365.25 ) ) / 30.6001 );
    my $k     = $m == 14 || $m == 15 ? 1 : 0;
    my $year  = $y + $k;
    my $mon   = $m - 1 - $k * 12 - 1;
    my $mday  = $mjd - 14956 - int( $y * 365.25 ) - int( $m * 30.6001 );
    my $epoch = mktime( 0, 0, 1, $mday, $mon, $year, 0, 0, 0 ) + bcd2int($bcd);
    return $epoch;
}

=head1 AUTHOR

Bojan Ramsak, C<< <BojanR@gmx.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dvb-epg at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DVB-Epg>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DVB::Epg

You can also look for information at:

=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2012 Bojan Ramsak.

This program is free software; you can redistribute it and/or modify it 
under the terms of the Artistic License v2.0

See http://www.opensource.org/licenses/Artistic-2.0 for more information.

=cut

1;    # End of DVB::Epg
