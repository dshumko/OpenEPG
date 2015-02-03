#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Std;
use DVB::Epg;
use DVB::Carousel;
use Carp;
use Time::Local;
use DBD::Firebird; 
use Encode;
use utf8;
use POSIX qw(ceil);
use Time::HiRes qw(usleep time);
use Config::INI::Reader;
use Cwd;
use threads;
use IO::Socket::Multicast;

my %epg_config = ();

$epg_config{"DB_NAME"} = 'localhost:epg';
$epg_config{"DB_USER"} = 'SYSDBA';
$epg_config{"DB_PSWD"} = 'masterkey';
$epg_config{"BIND_IP"} = '0.0.0.0';
$epg_config{"DAYS"}    = 7; # На сколько дней формировать EIT
$epg_config{"TMP"}     = cwd; # Куда сохранять времменые файлы
$epg_config{"RELOAD_TIME"} = 5; # Через сколько минут перечитывать поток
$epg_config{"EXPORT_TS"}   = '0'; # Экспортировать TS в файл
$epg_config{"NETWORK_ID"}  = ''; # ID сети с которой работает генератор

# Прочитаем EPG.INI  и заменим дефлтные настройки значениями с файла. раздел "EPG" 
my $ini = Config::INI::Reader->read_file('openepg.ini');
if (exists $ini->{'EPG'}) {
    %epg_config = (%epg_config, %{$ini->{'EPG'}});
}

# проверим чтоб дерриктория была со слешем в конце
if ((substr($epg_config{"TMP"}, -1) ne '/') and (substr($epg_config{"TMP"}, -1) ne '\\')) {
    $epg_config{"TMP"} = $epg_config{"TMP"}."/"; # добавим закрывающий слэш
}
mkdir $epg_config{"TMP"};

my $fbDb = DBI->connect("dbi:Firebird:db=".$epg_config{"DB_NAME"}.";ib_charset=UTF8", 
                        $epg_config{"DB_USER"}, 
                        $epg_config{"DB_PSWD"}, 
                        { RaiseError => 1, PrintError => 1, AutoCommit => 1, ib_enable_utf8 => 1 } );
my $sel_q = " select s.Dvbs_Id, coalesce(n.Aostrm,0) Aostrm, lower(n.Country) Country ".
            " from Dvb_Network n inner join Dvb_Streams s on (s.Dvbn_Id = n.Dvbn_Id)";
if ($epg_config{"NETWORK_ID"} eq '') {
    $sel_q = $sel_q . " where n.Dvbn_Id in (select first 1 d.Dvbn_Id from Dvb_Network d) "; 
}
else {
    $sel_q = $sel_q . " where n.NID = ".$epg_config{"NETWORK_ID"}; 
}
my $sth_s = $fbDb->prepare($sel_q);
$sth_s->execute or die "ERROR: Failed execute SQL!";
my @threads;
while (my ($dvbs_id, $aostrm, $country) = $sth_s->fetchrow_array()) {
    $epg_config{"ACTUAL_OTHER"} = $aostrm; # Передавать ли текущий/следующий поток в одном UDP потоке
    $epg_config{"COUNTRY"} = $country;     # язык по-умолчанию
    $epg_config{"DVBS_ID"} = $dvbs_id;
    push @threads, threads->create(\&RunThread, %epg_config);
    #RunThread(%epg_config); #for debug run without threads
} 
$fbDb->disconnect();

# Не дадим завершиться программе, пока работают все потоки
foreach my $thread (@threads) {
    $thread->join();
}

sub RunThread {
    my %cfg = @_;
    
    my $dvbsid = $cfg{"DVBS_ID"};
    my $eitDb      = $cfg{"TMP"}.'eit'.$dvbsid.'.sqlite';
    my $carouselDb = $cfg{"TMP"}.'carousel'.$dvbsid.'.sqlite';
    
    my $tsEpg = DVB::Epg->new( $eitDb ) || die( "Error opening EIT database [$eitDb]: $!");
    my $tsCarousel = DVB::Carousel->new( $carouselDb ) || die( "Error opening carousel database [$carouselDb]: $!");
    
    my $tsDb = DBI->connect("dbi:Firebird:db=".$cfg{"DB_NAME"}.";ib_charset=UTF8", 
                        $cfg{"DB_USER"}, $cfg{"DB_PSWD"}, 
                        { RaiseError => 1, PrintError => 1, AutoCommit => 1, ib_enable_utf8 => 1 } );
    
    my $sel_q = "select s.Es_Ip, s.Es_Port from Dvb_Streams s where s.Dvbs_Id = $dvbsid";
    my $sth_s = $tsDb->prepare($sel_q);
    $sth_s->execute or die "ERROR: Failed execute SQL!";
    my ($UDPhost, $UDPport) = $sth_s->fetchrow_array();
    
    InitEitDb($tsDb, %cfg);
    
    my $tsSocket = IO::Socket::Multicast->new(Proto=>'udp') || die "Couldn't open socket";
    
    if ( $cfg{"BIND_IP"} ne '0.0.0.0') {
        my $inet_addr = inet_aton($cfg{"BIND_IP"});
        $tsSocket->mcast_if($inet_addr);
    }
    $tsSocket->mcast_ttl(10);
    $tsSocket->mcast_loopback(0);
    $tsSocket->mcast_dest($UDPhost.':'.$UDPport);
    
    ReadEpgData($tsEpg, $tsCarousel, $tsDb, %cfg);
    while (1) {
        BuildEPG($tsEpg, $tsCarousel,  %cfg);
        
        if ($cfg{"EXPORT_TS"} eq '1') {
            my $pes = $tsEpg->getEit( 18, 30 );
            open( my $ts, ">", $epg_config{"TMP"}."eit$dvbsid.ts" ) || die "Error exporting TS chunk";
            binmode( $ts) ;
            print( $ts $pes );
            close( $ts );
        }
        
        SendUDP($tsCarousel, $tsSocket, %cfg);
    }
    
    $tsDb->disconnect();
}

sub InitEitDb {
    my ($tsDb, %cfg) = @_;
    
    my $dvbsid     = $cfg{"DVBS_ID"};
    my $eitDb      = $cfg{"TMP"}.'eit'.$dvbsid.'.sqlite';
    my $carouselDb = $cfg{"TMP"}.'carousel'.$dvbsid.'.sqlite';
    
    my $tsEpg = DVB::Epg->new( $eitDb ) || die( "Error opening EIT database [$eitDb]: $!");
    my $tsCarousel = DVB::Carousel->new( $carouselDb ) || die( "Error opening carousel database [$carouselDb]: $!");

    $tsEpg->initdb() || die( "Initialization of EIT database failed");
    
    my $number_of_segments  = $cfg{"DAYS"}*8; #(3days*8)
    
    my $sel_q = " select
                    sc.Sid, n.Onid, s.Tsid, n.Nid, sc.Ch_Id, iif(s.Dvbs_Id = $dvbsid, 1, 0) as isactual
                  from Dvb_Network n
                       inner join Dvb_Streams s on (n.Dvbn_Id = s.Dvbn_Id)
                       inner join Dvb_Stream_Channels sc on (s.Dvbs_Id = sc.Dvbs_Id)";
    # Будем ли передавать данные другого потока
    if ($cfg{"ACTUAL_OTHER"} == 1) {
        $sel_q = $sel_q." where n.Dvbn_Id in (select a.Dvbn_Id from Dvb_Streams a where a.Dvbs_Id = $dvbsid) ";
    }
    else {
        $sel_q = $sel_q." where s.Dvbs_Id = $dvbsid ";
    }
    
    my $sth_s = $tsDb->prepare($sel_q);
    $sth_s->execute or die "ERROR: Failed execute SQL!";
    while ( my ($sid, $onid, $tsid, $nid, $chid, $isactual) = $sth_s->fetchrow_array()) {
        $tsEpg->addEit( 18, $sid, $onid, $tsid, $chid, $number_of_segments, $isactual, '');
    } 
    
    $tsCarousel->initdb() || die( "Initialization of carousel database failed");
}

sub ReadEpgData {
    my ($tsEPG, $tsCarousel, $tsDb, %cfg) = @_;
    
    my $dvbsid     = $cfg{"DVBS_ID"};
    my $sel_q = "select 
                   ch_id, date_start, date_stop, title, description, minage, lower(lang)
                 from Get_Epg($dvbsid, null, null, ".$cfg{"ACTUAL_OTHER"}.")";
    my $sth_s = $tsDb->prepare($sel_q);
    $sth_s->execute or die "ERROR: Failed execute SQL!";
    
    while (my ($program, $start, $stop, $title, $synopsis, $minage, $lang) = $sth_s->fetchrow_array()) {
        #lang codes http://en.wikipedia.org/wiki/List_of_ISO_639-2_codes
        if (!defined $lang) { 
            $lang = $cfg{"COUNTRY"}
        }
        my $event;
        
        if( $start =~ /^(\d+).(\d+).(\d+)\s+(\d+):(\d+):(\d+)$/) {
            my @t = ( $6, $5, $4, $1, $2-1, $3);
            $event->{start} = timelocal(@t);
        }        
        else {
            die( "Incorrect start time [$start]");
        }
        
        if( $stop =~ /^(\d+).(\d+).(\d+)\s+(\d+):(\d+):(\d+)$/) {
            my @t = ( $6, $5, $4, $1, $2-1, $3);
            $event->{stop} = timelocal(@t);
        }        
        else {
            die( "Incorrect start time [$start]");
        }
        
        $event->{uid} = $program;  
        $event->{service_id} = $program;  
        
        my ($to_code_page, $lang_prefix, $title_ISO, $synopsis_ISO);
        
        # iso codes http://en.wikipedia.org/wiki/ISO/IEC_8859
        #
        # define codepage according to Annex.2 of EN 300 468
        # 0x10 0x00 0x01 ISO/IEC 8859-1 [23] West European
        # 0x10 0x00 0x02 ISO/IEC 8859-2 [24] East European
        # 0x10 0x00 0x03 ISO/IEC 8859-3 [25] South European
        # 0x10 0x00 0x04 ISO/IEC 8859-4 [26] North and North-East European
        # 0x10 0x00 0x05 ISO/IEC 8859-5 [27] Latin/Cyrillic A.2
        # 0x10 0x00 0x06 ISO/IEC 8859-6 [28] Latin/Arabic A.3
        # 0x10 0x00 0x07 ISO/IEC 8859-7 [29] Latin/Greek A.4
        # 0x10 0x00 0x08 ISO/IEC 8859-8 [30] Latin/Hebrew A.5
        # 0x10 0x00 0x09 ISO/IEC 8859-9 [31] West European & Turkish A.6
        # 0x10 0x00 0x0A ISO/IEC 8859-10 [32] North European A.7
        # 0x10 0x00 0x0B ISO/IEC 8859-11 [33] Thai A.8
        # 0x10 0x00 0x0D ISO/IEC 8859-13 [34] Baltic A.9
        # 0x10 0x00 0x0E ISO/IEC 8859-14 [35] Celtic A.10
        # 0x10 0x00 0x0F ISO/IEC 8859-15 [36] West European A.11
        if   ($lang eq 'rus') { $title_ISO = encode("iso-8859-5", $title); $lang_prefix  = "\x10\x00\x5"; } # Russian
        elsif($lang eq 'bel') { $title_ISO = encode("iso-8859-5", $title); $lang_prefix  = "\x10\x00\x5"; } # Belarusian
        elsif($lang eq 'ukr') { $title_ISO = encode("iso-8859-5", $title); $lang_prefix  = "\x10\x00\x5"; } # Ukrainian
        elsif($lang eq 'eng') { $title_ISO = encode("iso-8859-1", $title); $lang_prefix  = "\x10\x00\x1"; } # English
        elsif($lang eq 'lav') { $title_ISO = encode("iso-8859-4", $title); $lang_prefix  = "\x10\x00\x4"; } # Latvian
        elsif($lang eq 'lit') { $title_ISO = encode("iso-8859-4", $title); $lang_prefix  = "\x10\x00\x4"; } # Lithuanian
        elsif($lang eq 'est') { $title_ISO = encode("iso-8859-4", $title); $lang_prefix  = "\x10\x00\x4"; } # Estonian
        elsif($lang eq 'pol') { $title_ISO = encode("iso-8859-2", $title); $lang_prefix  = "\x10\x00\x2"; } # Polish
        elsif($lang eq 'fra') { $title_ISO = encode("iso-8859-1", $title); $lang_prefix  = "\x10\x00\x1"; } # French
        elsif($lang eq 'deu') { $title_ISO = encode("iso-8859-1", $title); $lang_prefix  = "\x10\x00\x1"; } # German
        else {                  $title_ISO = encode("iso-8859-1", $title); $lang_prefix  = "\x10\x00\x1"; } # English
        
        #my $title_ISO    = encode("iso-8859-5", $title);
        $synopsis_ISO = encode("iso-8859-5", $synopsis);
        
        my @descriptors;
        my $short_descriptor;
        $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
        $short_descriptor->{language_code} = $lang; # language code from ISO 639-2 lowercase 
        $short_descriptor->{codepage_prefix} = $lang_prefix; 
        $short_descriptor->{event_name} = $title_ISO;
        $short_descriptor->{text} = "";
        push( @descriptors, $short_descriptor);
        
        if (defined $synopsis_ISO) {
            my $extended_descriptor;
            $extended_descriptor->{descriptor_tag} = 0x4e;    # extended event descriptor
            $extended_descriptor->{language_code} = $lang;
            $extended_descriptor->{codepage_prefix} = $lang_prefix;
            $extended_descriptor->{text} = $synopsis_ISO;
            push( @descriptors, $extended_descriptor);
        }
        
        if (defined $minage) {
            my $parental_descriptor;
            $parental_descriptor->{descriptor_tag} = 0x55;    # parental rating descriptor
                my $rate;
                $rate->{country_code} = $lang;
                $rate->{rating} = ($minage-3); # rating = age limitation - 3
                push( @{$parental_descriptor->{list}}, $rate);
            push( @descriptors, $parental_descriptor);
        }
        $event->{descriptors} = \@descriptors;
        
        $tsEPG->addEvent( $event);
    }
    
    return 1;
}

sub BuildEPG {
    my ($tsEPG, $tsCarousel, %cfg) = @_;
    
    my $pid = 18;
    my $interval = 30;  # calculate the chunk for 30 seconds
    
    if( $tsEPG->updateEit( $pid )) {
        # Extract the snippet 
        my $pes = $tsEPG->getEit( $pid, $interval );
        print $cfg{"DVBS_ID"}." bitrate = ".( length( $pes )*8/$interval/1000 )." kbps\n";
        $tsCarousel->addMts( $pid, \$pes, $interval*1000 );
    }
}

sub SendUDP {
    my ($carousel, $multicast, %cfg) = @_;
    my $continuityCounter = 0;
    my $start = time();
    
    my $reload_time = ($cfg{'RELOAD_TIME'})*60;
    while( 1 ) {
        # get all data fot the EIT
        my $meta = $carousel->getMts( 18 );
        
        if( ! defined $meta) {
            print "No MTS chunk found for playing\n";
            sleep( 1 );
            next;
        }
        
        # set the variables
        my $interval = $$meta[1];
        my $mts = $$meta[2];
        my $mtsCount = length( $mts) / 188;
        my $packetCounter = 0;
        
        # correct continuity counter    
        for ( my $j = 3 ; $j < length( $mts ) ; $j += 188 ) {
            substr( $mts, $j, 1, chr( 0b00010000 | ( $continuityCounter & 0x0f ) ) );
            $continuityCounter += 1;
        }
        
        # add stuffing packets to have a multiple of 7 packets in the buffer
        # Why 7 packets? 
        # 7 x 188 = 1316
        # Because 7 TS packets fit in a typical UDP packet.
        my $i = $mtsCount % 7;
        while ( $i > 0 && $i < 7) {
            $mts .= "\x47\x1f\xff\x10"."\xff" x 184;    # stuffing packet
            $i += 1;
        }
        
        # correct the count of packets
        $mtsCount = length( $mts) / 188;
        
        # calculate the waiting time between playing chunks of 7 packets in micro seconds
        my $gap = ceil( $interval / $mtsCount * 7 * 1000);
        
        # play packets of 7 x 188bytes
        while ($packetCounter < $mtsCount) {
            my $chunkCount = $mtsCount-$packetCounter;
            $chunkCount = 7 if $chunkCount > 7; 
            
            $multicast->mcast_send(substr( $mts, $packetCounter * 188, $chunkCount * 188));
            $packetCounter += $chunkCount;
            usleep( $gap );
        }
        my $end = time();
        if (($end - $start) > $reload_time) {
            last;
        }
    }
}

# end of file