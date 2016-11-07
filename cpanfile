requires 'perl', '5.010001';
requires 'strict';
requires 'warnings';
requires 'Exporter';
requires 'File::Spec::Functions';
requires 'Path::Tiny';
requires 'Carp';
requires 'Getopt::Long';
requires 'Pod::Usage';
requires 'Capture::Tiny';
requires 'Data::Dumper';
requires 'Log::Log4perl';
requires 'File::Find::Rule';
requires 'Config::Std';
requires 'POSIX';
requires 'HTTP::Tiny';
requires 'HTTP::ClickHouse';
requires 'PerlIO::gzip';
requires 'File::Temp';
requires 'DateTime::Tiny';
requires 'Parallel::ForkManager';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

on 'develop' => sub {
    recommends 'Regexp::Debugger';
};

