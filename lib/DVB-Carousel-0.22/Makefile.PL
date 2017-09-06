use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DVB::Carousel',
    AUTHOR              => q{Bojan Ramšak <BojanR@gmx.net>},
    VERSION_FROM        => 'lib/DVB/Carousel.pm',
    ABSTRACT_FROM       => 'lib/DVB/Carousel.pm',
    (eval { ExtUtils::MakeMaker->VERSION(6.3) } ? (LICENSE => 'perl') : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'DBD::SQLite' => 1.29,        
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DVB-Carousel-*' },
);
