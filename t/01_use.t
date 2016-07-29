#!/usr/bin/env perl
use strict;
use warnings;

use Test::More;

my $module = 'FindOrigin';
my @subs = qw( 
  run
  init_logging
  get_parameters_from_cmd
  _capture_output
  _exec_cmd
  _http_exec_query
  create_db
  import_blastout
  import_blastdb_stats
  import_names
);

use_ok( $module, @subs);

foreach my $sub (@subs) {
    can_ok( $module, $sub);
}

done_testing();
