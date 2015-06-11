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
use IO::File;

my %epg_config = ();

#Ищем OpenEPG.ini файл в каталоге со скриптом
my $ini_file = cwd.'/OpenEPG.ini';

$epg_config{"DB_NAME"} = 'localhost:epg';
$epg_config{"DB_USER"} = 'SYSDBA';
$epg_config{"DB_PSWD"} = 'masterkey';
$epg_config{"BIND_IP"} = '0.0.0.0';
$epg_config{"TS_NAME"} = '';        # Будем хранить TSID
$epg_config{"DAYS"}    = 7;         # На сколько дней формировать EIT
$epg_config{"TMP"}     = cwd;       # Куда сохранять времменые файлы
$epg_config{"RELOAD_TIME"} = 5;     # Через сколько минут перечитывать поток
$epg_config{"EXPORT_TS"}   = '0';   # Экспортировать TS в файл
$epg_config{"NETWORK_ID"}  = '';    # NID сети с которой работает генератор
$epg_config{"READ_EPG"}    = 60;    # Через сколько минут будем проверять данные в базе A4on.TV и если изменились перечитывать
$epg_config{"DESC_LEN"}    = 500;   # Количество символов в описании
$epg_config{"RUS_PAGE"}    = 1;     # Как кодировать язык. согласно EN 300 468, 
                                    # ISO/IEC 8859-5 [27] Latin/Cyrillic alphabe может быть 1 = \0x01 (Table A.3) , а может быть 2 = \0x10\0x00\0x5 (Table A.4)


# !!! не использовать, не готово !!!
$epg_config{"TEXT_IN_UTF"} = 0; # Передавать текст событий в UTF8 а не в ISO 
# при включении выдает ошибку - Character in 'C' format wrapped in pack at DVB/Epg.pm line 1353.

# Проверим, если ini файл с сигнатурой BOM, то удалим ее
my $fh = new IO::File "< $ini_file" or die "Cannot open $ini_file : $!";
binmode($fh);

my $buf;
my $bom = "\xef\xbb\xbf";
my $len = length($bom);
my $buflen = 3;

read($fh,$buf,$len);
if (substr($buf, 0, 3) eq substr($bom, 0, 3)) {
    my $fw = new IO::File "> $ini_file.new" or die "Cannot open $ini_file.new : $!";
    binmode($fw);
    my $buflen = (stat($fh))[7];
    while (read($fh,$buf,$buflen)) {
        print $fw $buf or die "Write to $ini_file failed: $!";
    }
    close($fw) or die "Error closing $ini_file.new : $!";
    close($fh) or die "Error closing $ini_file : $!";
    unlink($ini_file);
    rename "$ini_file.new", "$ini_file";
}
else {
    close($fh) or die "Error closing $ini_file : $!";
}

# Прочитаем EPG.INI  и заменим дефлтные настройки значениями с файла. раздел "EPG" 
my $ini = Config::INI::Reader->read_file($ini_file);
if (exists $ini->{'EPG'}) {
    %epg_config = (%epg_config, %{$ini->{'EPG'}});
}

if ($epg_config{"RUS_PAGE"} == 2) {
    $epg_config{"RUS_HEX"} = "\0x10\0x00\0x5";
}
else {
    $epg_config{"RUS_HEX"} = "\x01";
}

if ($epg_config{"DESC_LEN"} !~ /^\d+$/) {
    print "Wrong description length parameter. Set length as 500 \n";    
    $epg_config{"DESC_LEN"} = 500;
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
my $sel_q = " select s.Dvbs_Id, coalesce(n.Aostrm,0), lower(n.Country), s.Es_Ip UDPhost, s.Es_Port UDPport, coalesce(n.Descriptors,'') desc, 
                     coalesce((select list(distinct c.Tsid) from Dvb_Stream_Channels c where c.Dvbs_Id = s.Dvbs_Id), coalesce(s.Tsid,'no TSID')) tsname 
            from Dvb_Network n inner join Dvb_Streams s on (s.Dvbn_Id = n.Dvbn_Id)";
if ($epg_config{"NETWORK_ID"} eq '') {
    $sel_q = $sel_q . " where n.Dvbn_Id in (select first 1 d.Dvbn_Id from Dvb_Network d) "; 
}
else {
    $sel_q = $sel_q . " where n.NID = ".$epg_config{"NETWORK_ID"}; 
}
my $sth_s = $fbDb->prepare($sel_q);
$sth_s->execute or die "ERROR: Failed execute SQL Dvb_Network !";
my @threads;
while (my ($dvbs_id, $aostrm, $country, $UDPhost, $UDPport, $desc, $tsname) = $sth_s->fetchrow_array()) {
    $epg_config{"ACTUAL_OTHER"} = $aostrm; # Передавать ли текущий/следующий поток в одном UDP потоке
    $epg_config{"COUNTRY"} = $country;     # язык по-умолчанию
    $epg_config{"DVBS_ID"} = $dvbs_id;     
    $epg_config{"UDPhost"} = $UDPhost;
    $epg_config{"UDPport"} = $UDPport;
    $epg_config{"TS_NAME"} = $tsname;
    
    $epg_config{"SHOW_EXT"} = 0; # Передавать расш. описание
    $epg_config{"SHOW_AGE"} = 0; # Передавать возраст
    $epg_config{"SHOW_GNR"} = 0; # Передавать жанр
    
    if (index($desc, 'ExtendedEventDescriptor')>=0)  { $epg_config{"SHOW_EXT"} = "1"; }
    if (index($desc, 'ParentalRatingDescriptor')>=0) { $epg_config{"SHOW_AGE"} = "1"; }
    if (index($desc, 'ContentDescriptor')>=0)        { $epg_config{"SHOW_GNR"} = "1"; }
    
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
    
    my $dvbsid     = $cfg{"DVBS_ID"};
    my $eitDb      = $cfg{"TMP"}.'eit'.$dvbsid.'.sqlite';
    my $carouselDb = $cfg{"TMP"}.'carousel'.$dvbsid.'.sqlite';
    
    my $tsEpg = DVB::Epg->new( $eitDb ) || die( "Error opening EIT database [$eitDb]: $!");
    my $tsCarousel = DVB::Carousel->new( $carouselDb ) || die( "Error opening carousel database [$carouselDb]: $!");
    
    my $tsSocket = IO::Socket::Multicast->new(Proto=>'udp') || die "Couldn't open socket";
    
    if ( $cfg{"BIND_IP"} ne '0.0.0.0') {
        my $inet_addr = inet_aton($cfg{"BIND_IP"});
        $tsSocket->mcast_if($inet_addr);
    }
    $tsSocket->mcast_ttl(10);
    $tsSocket->mcast_loopback(0);
    $tsSocket->mcast_dest($cfg{"UDPhost"}.':'.$cfg{"UDPport"});
    
    my $lastCheckEPG = '';
    
    my $TimeToCheck = 0;
    while (1) {
        # пришло ли время проверять данные в базе A4on.TV
        if ($TimeToCheck <= 0) {
            my $tsDb = DBI->connect("dbi:Firebird:db=".$cfg{"DB_NAME"}.";ib_charset=UTF8", $cfg{"DB_USER"}, $cfg{"DB_PSWD"}, 
                                { RaiseError => 1, PrintError => 1, AutoCommit => 1, ib_enable_utf8 => 1 } );
            # Время в формате 26.02.2015 23:50:00
            my $attr = {
                ib_timestampformat => '%d.%m.%Y %H:%M:%S',
                ib_dateformat => '%d.%m.%Y',
                ib_timeformat => '%H:%M:%S',
            };
            # Прочитаем последнее изменение в БД
            my $sel_DVBs = "select Coalesce(s.EPG_UPDATED, current_timestamp) from Dvb_Streams s where s.Dvbs_Id = $dvbsid";
            my $sth_s = $tsDb->prepare($sel_DVBs, $attr);
            $sth_s->execute or die "ERROR: Failed execute SQL Dvb_Streams !";
            my ($EPGupdateON) = $sth_s->fetchrow_array();
            $sth_s->finish();
            # проверим совпадает ли с тем что мы уже проверили
            if ($lastCheckEPG ne $EPGupdateON) {
                print "TSID ".$cfg{"TS_NAME"}." EPG readed time $lastCheckEPG update time $EPGupdateON \n";
                InitEitDb($tsDb, %cfg);
                ReadEpgData($tsEpg, $tsCarousel, $tsDb, %cfg);
                $lastCheckEPG = $EPGupdateON;
            }
            
            $tsDb->disconnect();
            $TimeToCheck = $cfg{'READ_EPG'};
        }
        
        BuildEPG($tsEpg, $tsCarousel,  %cfg);
        
        if ($cfg{"EXPORT_TS"} eq '1') {
            my $pes = $tsEpg->getEit( 18, 30 );
            open( my $ts, ">", $epg_config{"TMP"}."eit$dvbsid.ts" ) || die "Error exporting TS chunk";
            binmode( $ts) ;
            print( $ts $pes );
            close( $ts );
        }
        SendUDP($tsCarousel, $tsSocket, %cfg);
        
        # Уменьшим счетчик времени при 0 или минусе будем заново формировать БД
        $TimeToCheck = $TimeToCheck - $cfg{'RELOAD_TIME'};
    }
    
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
    
    my $sel_q = " select sc.Sid, n.Onid, coalesce(sc.Tsid, s.Tsid) Tsid, n.Nid, sc.Ch_Id, iif(s.Dvbs_Id = $dvbsid, 1, 0) as isactual
                  from Dvb_Network n
                       inner join Dvb_Streams s on (n.Dvbn_Id = s.Dvbn_Id)
                       inner join Dvb_Stream_Channels sc on (s.Dvbs_Id = sc.Dvbs_Id) 
                  where (not sc.Sid is null) and ";
    # Будем ли передавать данные другого потока
    if ($cfg{"ACTUAL_OTHER"} == 1) {
        $sel_q = $sel_q." n.Dvbn_Id in (select a.Dvbn_Id from Dvb_Streams a where a.Dvbs_Id = $dvbsid) ";
    }
    else {
        $sel_q = $sel_q." s.Dvbs_Id = $dvbsid ";
    }
    
    my $sth_s = $tsDb->prepare($sel_q);
    $sth_s->execute or die "ERROR: Failed execute SQL Dvb_Stream_Channels !";
    while ( my ($sid, $onid, $tsid, $nid, $chid, $isactual) = $sth_s->fetchrow_array()) {
        $tsEpg->addEit( 18, $sid, $onid, $tsid, $chid, $number_of_segments, $isactual, '');
    } 
    
    $tsCarousel->initdb() || die( "Initialization of carousel database failed");
}

sub ReadEpgData {
    my ($tsEPG, $tsCarousel, $tsDb, %cfg) = @_;
    
    my $dvbsid     = $cfg{"DVBS_ID"};
    
    # Время в формате 26.02.2015 23:50:00
    my $attr = {
        ib_timestampformat => '%d.%m.%Y %H:%M:%S',
        ib_dateformat => '%d.%m.%Y',
        ib_timeformat => '%H:%M:%S',
    };
    
    my $sel_q = "select ch_id, date_start, date_stop, title, left(description, ".$cfg{"DESC_LEN"}.") description, minage, lower(lang), dvbgenres from Get_Epg($dvbsid, null, null, ".$cfg{"ACTUAL_OTHER"}.")";
    my $sth_s = $tsDb->prepare($sel_q, $attr);
    $sth_s->execute or die "ERROR: Failed execute SQL Get_Epg !";
    
    while (my ($program, $start, $stop, $title, $synopsis, $minage, $lang, $dvbgenres) = $sth_s->fetchrow_array()) {
        #lang codes http://en.wikipedia.org/wiki/List_of_ISO_639-2_codes
        if (!defined $lang) { 
            $lang = $cfg{"COUNTRY"}
        }
        my $event;
        
        if( $start =~ /^(\d+).(\d+).(\d+)\s+(\d+):(\d+):(\d+)$/) {
            my @t = ( $6, $5, $4, $1, $2-1, $3);
            $event->{start} = timegm(@t);
        }        
        else {
            die( "Incorrect start time [$start]");
        }
        
        if( $stop =~ /^(\d+).(\d+).(\d+)\s+(\d+):(\d+):(\d+)$/) {
            my @t = ( $6, $5, $4, $1, $2-1, $3);
            $event->{stop} = timegm(@t);
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
        
        if ($cfg{"TEXT_IN_UTF"} ne '1') {
            if  (($lang eq 'rus')    # Russian
                || ($lang eq 'bel')  # Belarusian
                || ($lang eq 'ukr')) # Ukrainian
            { 
                $title_ISO    = encode("iso-8859-5", $title); 
                $synopsis_ISO = encode("iso-8859-5", $synopsis);
                $lang_prefix  = $cfg{"RUS_HEX"};
            } 
            elsif(($lang eq 'lav')   # Latvian
                || ($lang eq 'lit')  # Lithuanian
                || ($lang eq 'est')) # Estonian
            {
                $title_ISO    = encode("iso-8859-4", $title); 
                $synopsis_ISO = encode("iso-8859-4", $synopsis);
                $lang_prefix  = "\x10\x00\x4";
            } 
            elsif($lang eq 'pol')    # Polish
            { 
                $title_ISO    = encode("iso-8859-2", $title); 
                $synopsis_ISO = encode("iso-8859-2", $synopsis);
                $lang_prefix  = "\x10\x00\x2";
            }
            else {                   # English German French
                $title_ISO    = encode("iso-8859-1", $title); 
                $synopsis_ISO = encode("iso-8859-1", $synopsis);
                $lang_prefix  = "\x10\x00\x1";
            } 
        }
        else {
            $title_ISO    = $title;
            $synopsis_ISO = $synopsis;
            $lang_prefix  = "\x15"; 
        }
        
        my @descriptors;
        my $short_descriptor;
        $short_descriptor->{descriptor_tag} = 0x4d; # short event descriptor
        $short_descriptor->{language_code} = $lang; # language code from ISO 639-2 lowercase 
        $short_descriptor->{codepage_prefix} = $lang_prefix; 
        $short_descriptor->{event_name} = $title_ISO;
        $short_descriptor->{text} = "";
        push( @descriptors, $short_descriptor);
        
        if ($cfg{"SHOW_EXT"} eq '1') {
            if (defined $synopsis_ISO) {
                my $extended_descriptor;
                $extended_descriptor->{descriptor_tag} = 0x4e;    # extended event descriptor
                $extended_descriptor->{language_code} = $lang;
                $extended_descriptor->{codepage_prefix} = $lang_prefix;
                $extended_descriptor->{text} = $synopsis_ISO;
                push( @descriptors, $extended_descriptor);
            }
        }
        
        if ($cfg{"SHOW_AGE"} eq '1') {
            if (defined $minage) {
                my $parental_descriptor;
                $parental_descriptor->{descriptor_tag} = 0x55;    # parental rating descriptor
                    my $rate;
                    $rate->{country_code} = $lang;
                    $rate->{rating} = ($minage-3); # rating = age limitation - 3
                    push( @{$parental_descriptor->{list}}, $rate);
                push( @descriptors, $parental_descriptor);
            }
        }
        
        if ($cfg{"SHOW_GNR"} eq '1') {
            if (defined $dvbgenres) {
                my $dvbgenres_descriptor;
                $dvbgenres_descriptor->{descriptor_tag} = 0x54;    # dvb genre descriptor
                my @values = split(',', $dvbgenres);
                    foreach my $val (@values) {
                    my $genre;
                    $genre->{dvb} = $val;
                    push( @{$dvbgenres_descriptor->{list}}, $genre);
                }
                push( @descriptors, $dvbgenres_descriptor);
            }
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
        printf("TSID %s bitrate %.3f kbps\n", $cfg{"TS_NAME"},  ( length( $pes )*8/$interval/1000 ));
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
            print "TSID ".$cfg{"TS_NAME"}." No MTS chunk found for playing\n";
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