use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DVB::Epg',
    AUTHOR              => q{Bojan Ramšak <BojanR@gmx.net>},
    VERSION_FROM        => 'lib/DVB/Epg.pm',
    ABSTRACT_FROM       => 'lib/DVB/Epg.pm',
    (eval { ExtUtils::MakeMaker->VERSION(6.3) } ? (LICENSE => 'perl') : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'DBD::SQLite' => 1.29,
        'Digest::CRC' => 0.16,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DVB-Epg-*' },
);
