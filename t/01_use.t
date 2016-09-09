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
  import_map
  import_blastdb_stats
  import_names
  blastout_uniq
  bl_uniq_expanded
  queue_and_run
  exclude_ti_from_blastout
);

use_ok( $module, @subs);

foreach my $sub (@subs) {
    can_ok( $module, $sub);
}

done_testing();
