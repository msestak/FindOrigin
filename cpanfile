requires 'perl', '5.008001';
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
requires 'Data::Printer';
requires 'Regexp::Debugger';
requires 'Log::Log4perl';
requires 'File::Find::Rule';
requires 'Config::Std';
requires 'HTTP::Tiny';
requires 'ClickHouse';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

on 'develop' => sub {
    recommends 'Regexp::Debugger';
};

