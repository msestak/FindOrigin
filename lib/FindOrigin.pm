#!/usr/bin/env perl
package FindOrigin;
use 5.010001;
use strict;
use warnings;
use Exporter 'import';
use File::Spec::Functions qw(:ALL);
use Path::Tiny;
use Carp;
use Getopt::Long;
use Pod::Usage;
use Capture::Tiny qw/capture/;
use Data::Dumper;
use Data::Printer;
#use Regexp::Debugger;
use Log::Log4perl;
use File::Find::Rule;
use Config::Std { def_sep => '=' };   #ClickHouse uses =
use POSIX qw(mkfifo);
use HTTP::Tiny;
use ClickHouse;
use PerlIO::gzip;
use File::Temp qw(tempfile);
use DateTime::Tiny;
use Parallel::ForkManager;

our $VERSION = "0.01";

our @EXPORT_OK = qw{
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
  bl_uniq_exp_iter
  queue_and_run
  exclude_ti_from_blastout
  import_blastdb
  dump_chdb
  restore_chdb
  top_hits

};

#MODULINO - works with debugger too
run() if !caller() or (caller)[0] eq 'DB';

### INTERFACE SUB starting all others ###
# Usage      : main();
# Purpose    : it starts all other subs and entire modulino
# Returns    : nothing
# Parameters : none (argument handling by Getopt::Long)
# Throws     : lots of exceptions from logging
# Comments   : start of entire module
# See Also   : n/a
sub run {
    croak 'main() does not need parameters' unless @_ == 0;

    #first capture parameters to enable verbose flag for logging
    my ($param_href) = get_parameters_from_cmd();

    #preparation of parameters
    my $verbose = $param_href->{verbose};
    my $quiet   = $param_href->{quiet};
    my @mode    = @{ $param_href->{mode} };

    #start logging for the rest of program (without capturing of parameters)
    init_logging( $verbose, $param_href->{argv} );
    ##########################
    # ... in some function ...
    ##########################
    my $log = Log::Log4perl::get_logger("main");

    # Logs both to Screen and File appender
    #$log->info("This is start of logging for $0");
    #$log->trace("This is example of trace logging for $0");

    #get dump of param_href if -v (verbose) flag is on (for debugging)
    my $param_print = sprintf( Data::Dumper->Dump( [ $param_href], [ qw(param_href) ] ) ) if $verbose;
    $log->debug( "$param_print" ) if $verbose;

    #call write modes (different subs that print different jobs)
    my %dispatch = (
        create_db            => \&create_db,               # drop and recreate database in ClickHouse
        import_blastout      => \&import_blastout,         # import BLAST output
        import_map           => \&import_map,              # import Phylostratigraphic map with header
        import_blastdb_stats => \&import_blastdb_stats,    # import BLAST database stats file
        import_names         => \&import_names,            # import names file
        blastout_uniq        => \&blastout_uniq,           # analyzes BLAST output file using map, names and blastout tables
        bl_uniq_expanded     => \&bl_uniq_expanded,        # add unique BLAST hits per species (at once, faster, large memory)
        bl_uniq_exp_iter     => \&bl_uniq_exp_iter,        # add unique BLAST hits per species (iteratively, slower, less memory)
        import_blastdb       => \&import_blastdb,          # import BLAST database with all columns
        queue_and_run        => \&queue_and_run,           # runs all steps for all blastout files
        exclude_ti_from_blastout =>
                            \&exclude_ti_from_blastout,    # excludes specific tax_id from BLAST output file
        dump_chdb            => \&dump_chdb,               # exports one table or all tables from database
        restore_chdb         => \&restore_chdb,            # imports one table or all tables from file into a database
        top_hits             => \&top_hits,                # selects top_hits from expanded tables

    );

    foreach my $mode (@mode) {
        if ( exists $dispatch{$mode} ) {
            $log->info( "RUNNING ACTION for mode: ", $mode );

            $dispatch{$mode}->($param_href);

            $log->info("TIME when finished for: $mode");
        }
        else {
            #complain if mode misspelled or just plain wrong
            $log->logcroak("Unrecognized mode --mode={$mode} on command line thus aborting");
        }
    }

    return;
}

### INTERNAL UTILITY ###
# Usage      : my ($param_href) = get_parameters_from_cmd();
# Purpose    : processes parameters from command line
# Returns    : $param_href --> hash ref of all command line arguments and files
# Parameters : none -> works by argument handling by Getopt::Long
# Throws     : lots of exceptions from die
# Comments   : works without logger
# See Also   : run()
sub get_parameters_from_cmd {

    #no logger here
    # setup config file location
    my ( $volume, $dir_out, $perl_script ) = splitpath($0);
    $dir_out = rel2abs($dir_out);
    my ($app_name) = $perl_script =~ m{\A(.+)\.(?:.+)\z};
    $app_name = lc $app_name;
    my $config_file = catfile( $volume, $dir_out, $app_name . '.cnf' );
    $config_file = canonpath($config_file);

    #read config to setup defaults
    read_config( $config_file => my %config );

    #p(%config);
    my $config_ps_href = $config{PS};

    #p($config_ps_href);
    my $config_ti_href = $config{TI};

    #p($config_ti_href);
    my $config_psname_href = $config{PSNAME};

    #push all options into one hash no matter the section
    my %opts;
    foreach my $key ( keys %config ) {

        # don't expand PS, TI or PSNAME
        next if ( ( $key eq 'PS' ) or ( $key eq 'TI' ) or ( $key eq 'PSNAME' ) );

        # expand all other options
        %opts = ( %opts, %{ $config{$key} } );
    }

    # put config location to %opts
    $opts{config} = $config_file;

    # put PS and TI section to %opts
    $opts{ps}     = $config_ps_href;
    $opts{ti}     = $config_ti_href;
    $opts{psname} = $config_psname_href;

    #cli part
    my @arg_copy = @ARGV;
    my ( %cli, @mode );
    $cli{quiet}   = 0;
    $cli{verbose} = 0;
    $cli{argv}    = \@arg_copy;

    #mode, quiet and verbose can only be set on command line
    GetOptions(
        # general options
        'help|h'       => \$cli{help},
        'man|m'        => \$cli{man},
        'config|cnf=s' => \$cli{config},
        'in|i=s'       => \$cli{in},
        'infile|if=s'  => \$cli{infile},
        'out|o=s'      => \$cli{out},
        'outfile|of=s' => \$cli{outfile},

        # files
        'blastout=s' => \$cli{blastout},
        'blastdb=s'  => \$cli{blastdb},
        'names|na=s' => \$cli{names},
        'map=s'      => \$cli{map},
        'stats=s'    => \$cli{stats},

        # tax_id
        'tax_id|ti=i' => \$cli{tax_id},

        # tables in database
        'blastout_tbl=s'   => \$cli{blastout_tbl},
        'blastdb_tbl=s'    => \$cli{blastdb_tbl},
        'names_tbl=s'      => \$cli{names_tbl},
        'map_tbl=s'        => \$cli{map_tbl},
        'stats_ps_tbl=s'   => \$cli{stats_ps_tbl},
        'stats_gen_tbl=s'  => \$cli{stats_gen_tbl},
        'report_ps_tbl=s'  => \$cli{report_ps_tbl},
        'report_exp_tbl=s' => \$cli{report_exp_tbl},

        # table and format for export/restore
        'table_ch=s'      => \$cli{table_ch},
        'format_ex=s'     => \$cli{format_ex},
        'max_processes=i' => \$cli{max_processes},
        'drop_tbl|drop'   => \$cli{drop_tbl}, #flag
        'tbl_sql=s'       => \$cli{tbl_sql},
        'tbl_contents=s'  => \$cli{tbl_contents},

        # top hits
        'top_hits=i'      => \$cli{top_hits},

        # connection parameters
        'host|ho=s'    => \$cli{host},
        'database|d=s' => \$cli{database},
        'user|u=s'     => \$cli{user},
        'password|p=s' => \$cli{password},
        'port|po=i'    => \$cli{port},

        # mode of action and verbosity
        'mode|mo=s{1,}' => \$cli{mode},       #accepts 1 or more arguments
        'quiet|q'       => \$cli{quiet},      #flag
        'verbose+'      => \$cli{verbose},    #flag
    ) or pod2usage( -verbose => 1 );

    # help and man
    pod2usage( -verbose => 1 ) if $cli{help};
    pod2usage( -verbose => 2 ) if $cli{man};

    #you can specify multiple modes at the same time
    @mode = split( /,/, $cli{mode} );
    $cli{mode} = \@mode;
    die 'No mode specified on command line' unless $cli{mode};    #DIES here if without mode

    #if not -q or --quiet print all this (else be quiet)
    if ( $cli{quiet} == 0 ) {

        #print STDERR 'My @ARGV: {', join( "} {", @arg_copy ), '}', "\n";
        #no warnings 'uninitialized';
        #print STDERR "Extra options from config:", Dumper(\%opts);

        if ( $cli{in} ) {
            say 'My input path: ', canonpath( $cli{in} );
            $cli{in} = rel2abs( $cli{in} );
            $cli{in} = canonpath( $cli{in} );
            say "My absolute input path: $cli{in}";
        }
        if ( $cli{infile} ) {
            say 'My input file: ', canonpath( $cli{infile} );
            $cli{infile} = rel2abs( $cli{infile} );
            $cli{infile} = canonpath( $cli{infile} );
            say "My absolute input file: $cli{infile}";
        }
        if ( $cli{out} ) {
            say 'My output path: ', canonpath( $cli{out} );
            $cli{out} = rel2abs( $cli{out} );
            $cli{out} = canonpath( $cli{out} );
            say "My absolute output path: $cli{out}";
        }
        if ( $cli{outfile} ) {
            say 'My outfile: ', canonpath( $cli{outfile} );
            $cli{outfile} = rel2abs( $cli{outfile} );
            $cli{outfile} = canonpath( $cli{outfile} );
            say "My absolute outfile: $cli{outfile}";
        }
    }
    else {
        $cli{verbose} = -1;    #and logging is OFF

        if ( $cli{in} ) {
            $cli{in} = rel2abs( $cli{in} );
            $cli{in} = canonpath( $cli{in} );
        }
        if ( $cli{infile} ) {
            $cli{infile} = rel2abs( $cli{infile} );
            $cli{infile} = canonpath( $cli{infile} );
        }
        if ( $cli{out} ) {
            $cli{out} = rel2abs( $cli{out} );
            $cli{out} = canonpath( $cli{out} );
        }
        if ( $cli{outfile} ) {
            $cli{outfile} = rel2abs( $cli{outfile} );
            $cli{outfile} = canonpath( $cli{outfile} );
        }
    }

    #copy all config opts
    my %all_opts = %opts;

    #update with cli options
    foreach my $key ( keys %cli ) {
        if ( defined $cli{$key} ) {
            $all_opts{$key} = $cli{$key};
        }
    }

    return ( \%all_opts );
}

### INTERNAL UTILITY ###
# Usage      : init_logging();
# Purpose    : enables Log::Log4perl log() to Screen and File
# Returns    : nothing
# Parameters : verbose flag + copy of parameters from command line
# Throws     : croaks if it receives parameters
# Comments   : used to setup a logging framework
#            : logfile is in same directory and same name as script -pl +log
# See Also   : Log::Log4perl at https://metacpan.org/pod/Log::Log4perl
sub init_logging {
    croak 'init_logging() needs verbose parameter' unless @_ == 2;
    my ( $verbose, $argv_copy ) = @_;

    #create log file in same dir where script is running
	#removes perl script and takes absolute path from rest of path
	my ($volume,$dir_out,$perl_script) = splitpath( $0 );
	#say '$dir_out:', $dir_out;
	$dir_out = rel2abs($dir_out);
	#say '$dir_out:', $dir_out;

    my ($app_name) = $perl_script =~ m{\A(.+)\.(?:.+)\z};   #takes name of the script and removes .pl or .pm or .t
    #say '$app_name:', $app_name;
    my $logfile = catfile( $volume, $dir_out, $app_name . '.log' );    #combines all of above with .log
	#say '$logfile:', $logfile;
	$logfile = canonpath($logfile);
	#say '$logfile:', $logfile;

    #colored output on windows
    my $osname = $^O;
    if ( $osname eq 'MSWin32' ) {
        require Win32::Console::ANSI;                                 #require needs import
        Win32::Console::ANSI->import();
    }

    #enable different levels based on verbose flag
    my $log_level;
    if    ($verbose == 0)  { $log_level = 'INFO';  }
    elsif ($verbose == 1)  { $log_level = 'DEBUG'; }
    elsif ($verbose == 2)  { $log_level = 'TRACE'; }
    elsif ($verbose == -1) { $log_level = 'OFF';   }
	else                   { $log_level = 'INFO';  }

    #levels:
    #TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    ###############################################################################
    #                              Log::Log4perl Conf                             #
    ###############################################################################
    # Configuration in a string ...
    my $conf = qq(
      log4perl.category.main                   = TRACE, Logfile, Screen

	  # Filter range from TRACE up
	  log4perl.filter.MatchTraceUp               = Log::Log4perl::Filter::LevelRange
      log4perl.filter.MatchTraceUp.LevelMin      = TRACE
      log4perl.filter.MatchTraceUp.LevelMax      = FATAL
      log4perl.filter.MatchTraceUp.AcceptOnMatch = true

      # Filter range from $log_level up
      log4perl.filter.MatchLevelUp               = Log::Log4perl::Filter::LevelRange
      log4perl.filter.MatchLevelUp.LevelMin      = $log_level
      log4perl.filter.MatchLevelUp.LevelMax      = FATAL
      log4perl.filter.MatchLevelUp.AcceptOnMatch = true
      
	  # setup of file log
      log4perl.appender.Logfile           = Log::Log4perl::Appender::File
      log4perl.appender.Logfile.filename  = $logfile
      log4perl.appender.Logfile.mode      = append
      log4perl.appender.Logfile.autoflush = 1
      log4perl.appender.Logfile.umask     = 0022
      log4perl.appender.Logfile.header_text = INVOCATION:$0 @$argv_copy
      log4perl.appender.Logfile.layout    = Log::Log4perl::Layout::PatternLayout
      log4perl.appender.Logfile.layout.ConversionPattern = [%d{yyyy/MM/dd HH:mm:ss,SSS}]%5p> %M line:%L==>%m%n
	  log4perl.appender.Logfile.Filter    = MatchTraceUp
      
	  # setup of screen log
      log4perl.appender.Screen            = Log::Log4perl::Appender::ScreenColoredLevels
      log4perl.appender.Screen.stderr     = 1
      log4perl.appender.Screen.layout     = Log::Log4perl::Layout::PatternLayout
      log4perl.appender.Screen.layout.ConversionPattern  = [%d{yyyy/MM/dd HH:mm:ss,SSS}]%5p> %M line:%L==>%m%n
	  log4perl.appender.Screen.Filter     = MatchLevelUp
    );

    # ... passed as a reference to init()
    Log::Log4perl::init( \$conf );

    return;
}


### INTERNAL UTILITY ###
# Usage      : my ($stdout, $stderr, $exit) = _capture_output( $cmd, $param_href );
# Purpose    : accepts command, executes it, captures output and returns it in vars
# Returns    : STDOUT, STDERR and EXIT as vars
# Parameters : ($cmd_to_execute,  $param_href)
# Throws     : nothing
# Comments   : second param is verbose flag (default off)
# See Also   :
sub _capture_output {
    my $log = Log::Log4perl::get_logger("main");
    $log->logdie( '_capture_output() needs a $cmd' ) unless (@_ ==  2 or 1);
    my ($cmd, $param_href) = @_;

    my $verbose = $param_href->{verbose};
    $log->debug(qq|Report: COMMAND is: $cmd|);

    my ( $stdout, $stderr, $exit ) = capture {
        system($cmd );
    };

    if ($verbose == 2) {
        $log->trace( 'STDOUT is: ', "$stdout", "\n", 'STDERR  is: ', "$stderr", "\n", 'EXIT   is: ', "$exit" );
    }

    return  $stdout, $stderr, $exit;
}

### INTERNAL UTILITY ###
# Usage      : _exec_cmd($cmd_git, $param_href, $cmd_info);
# Purpose    : accepts command, executes it and checks for success
# Returns    : prints info
# Parameters : ($cmd_to_execute, $param_href)
# Throws     : 
# Comments   : second param is verbose flag (default off)
# See Also   :
sub _exec_cmd {
    my $log = Log::Log4perl::get_logger("main");
    $log->logdie( '_exec_cmd() needs a $cmd, $param_href and info' ) unless (@_ ==  2 or 3);
	croak( '_exec_cmd() needs a $cmd' ) unless (@_ == 2 or 3);
    my ($cmd, $param_href, $cmd_info) = @_;
	if (!defined $cmd_info) {
		($cmd_info)  = $cmd =~ m/\A(\w+)/;
	}
    my $verbose = $param_href->{verbose};

    my ($stdout, $stderr, $exit) = _capture_output( $cmd, $param_href );
    if ($exit == 0 and $verbose > 1) {
        $log->trace( "$cmd_info success!" );
    }
	else {
        $log->trace( "$cmd_info failed!" );
	}
	return $exit;
}


## INTERNAL UTILITY ###
# Usage      : my ($success_del, $res_del) = _http_exec_query( { query => $query_del, %$param_href } );
# Purpose    : executes a query in ClickHouse using http connection
# Returns    : success and result of query
# Parameters : query and rest of database params
# Throws     : HTTP::Tiny errors and warnings
# Comments   : utility function to run queries in Clickhouse
# See Also   : 
sub _http_exec_query {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( '_http_exec_query() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;
	
    my $query    = $param_href->{query} or $log->logcroak('no $query sent to _http_exec_query()!');
    my $user     = defined $param_href->{user}     ? $param_href->{user}     : 'default';
    my $password = defined $param_href->{password} ? $param_href->{password} : '';
	my $host     = defined $param_href->{host}     ? $param_href->{host}     : 'localhost';
    my $port     = defined $param_href->{port}     ? $param_href->{port}     : 8123;

	my $url = 'http://' . $host . ':' . $port . '/';
 
    my $http = HTTP::Tiny->new();
	#print Dumper $http;
    #print "\n";
    
    my $response = $http->request('POST', $url, { content => qq{$query} } );
	#print Dumper $response;

	my $result;
    if ($response->{success}) {
    	
    	#print Dumper $response;
    
    	#print content
    	$result = $response->{content};
		#print $result;
    
    } else {
        $log->error( "Error: $response->{status} $response->{reasons}" );
    }

    $log->trace( "Report: sent $query to $url" );

    return $response->{success}, $result;
}



### INTERFACE SUB ###
# Usage      : --mode=create_db -d test_db_here
# Purpose    : creates database in ClickHouse
# Returns    : nothing
# Parameters : ( $param_href ) -> params from command line to connect to ClickHouse
#            : -d (database name)
# Throws     : croaks if wrong number of parameters
# Comments   : run only once at start (it drops database)
# See Also   :
sub create_db {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('create_db() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $database     = $param_href->{database} or $log->logcroak('no $database specified on command line!');
    my $query_del    = qq{DROP DATABASE IF EXISTS $database};
    my $query_create = qq{CREATE DATABASE IF NOT EXISTS $database};

    # drop database
    my ( $success_del, $res_del ) = _http_exec_query( { query => $query_del, %$param_href } );
    $log->error("Action: dropping $database failed!") unless $success_del;
    $log->debug("Action: database $database dropped successfully!") if $success_del;

    # create database
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Action: creating $database failed!") unless $success_create;
    $log->info("Action: database {$database} created successfully!") if $success_create;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastout
# Purpose    : imports BLAST output to ClickHouse database
# Returns    : blastout table name
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   :
# See Also   :
sub import_blastout {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_blastout() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{blastout} or $log->logcroak('no --blastout=filename specified on command line!');
    my $table = path($infile)->basename;
    $table =~ s/\./_/g;    #for files that have dots in name
    $table =~ s/_gz//g;    #for .gz

    # drop and recreate table where we are importing
    my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$table};
    my ( $success_drop, $res_drop ) = _http_exec_query( { query => $query_drop, %$param_href } );
    $log->error("Error: dropping $table failed!") unless $success_drop;
    $log->debug("Action: table $table dropped successfully!") if $success_drop;

    # wait a sec so it has time to execute
    sleep 1;

    # create table
    my $columns
      = q{(prot_id String, blast_hit String, perc_id Float32, alignment_length UInt32, mismatches UInt32, gap_openings UInt32, query_start UInt32, query_end UInt32, subject_start UInt32, subject_end UInt32, e_value Float64, bitscore Float32, date Date DEFAULT today())};
    my $engine       = q{ENGINE=MergeTree(date, prot_id, 8192)};
    my $query_create = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$table $columns $engine};
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Error: creating $table failed!") unless $success_create;
    $log->debug("Action: table $table created successfully!") if $success_create;

    # wait a sec so it has time to execute
    sleep 1;

    # import into table (in ClickHouse) from gziped file (needs pigz)
    my $import_query
      = qq{INSERT INTO $param_href->{database}.$table (prot_id, blast_hit, perc_id, alignment_length, mismatches, gap_openings, query_start, query_end, subject_start, subject_end, e_value, bitscore) FORMAT TabSeparated};
    my $import_cmd = qq{ pigz -c -d $infile | clickhouse-client --query "$import_query"};
    my ( $stdout, $stderr, $exit ) = _capture_output( $import_cmd, $param_href );
    if ( $exit == 0 ) {
        $log->debug("Action: import to $param_href->{database}.$table success!");
    }
    else {
        $log->logdie("Error: $import_cmd failed: $stderr");
    }

    # wait a sec so it has time to execute
    sleep 1;

    # check number of rows inserted
    my $query_cnt = qq{SELECT count() FROM $param_href->{database}.$table};
    my ( $success_cnt, $res_cnt ) = _http_exec_query( { query => $query_cnt, %$param_href } );
    $res_cnt =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: counting rows in $table failed!") unless $success_cnt;
    $log->info("Action: inserted $res_cnt rows into {$table}") if $success_cnt;

    return $table;
}

### INTERNAL UTILITY ###
# Usage      : --mode=import_map on command line
# Purpose    : imports map with header format and psname (.phmap_names)
# Returns    : name of the map_tbl
# Parameters : full path to map file and database connection parameters
# Throws     : croaks if wrong number of parameters
# Comments   : creates temp files without header for final load
# See Also   :
sub import_map {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_map() needs {$param_href}') unless @_ == 1;
    my ($param_href) = @_;

    # check required parameters
    if ( !exists $param_href->{map} ) { $log->logcroak('no --map=filename specified on command line!'); }

    # get name of map table
    my $map_tbl = path( $param_href->{map} )->basename;
    ($map_tbl) = $map_tbl =~ m/\A([^\.]+)\.phmap_names\z/;
    $map_tbl .= '_map';
    $map_tbl =~ s/\./_/g;    #for files that have dots in name

    # create tmp filename in same dir as input map with header
    my $temp_map = path( path( $param_href->{map} )->parent, $map_tbl );
    open( my $tmp_fh, ">", $temp_map ) or $log->logdie("Error: can't open map $temp_map for writing:$!");

    # need to skip header
    open( my $map_fh, "<", $param_href->{map} )
      or $log->logdie("Error: can't open map $param_href->{map} for reading:$!");
    while (<$map_fh>) {
        chomp;

        # check if record (ignore header)
        next if !/\A(?:[^\t]+)\t(?:[^\t]+)\t(?:[^\t]+)\t(?:[^\t]+)\z/;

        my ( $prot_id, $ps, $ti, $ps_name ) = split "\t", $_;

        # this is needed because psname can be short without {cellular_organisms : Eukaryota}
        my $psname_short;
        if ( $ps_name =~ /:/ ) {    # {cellular_organisms : Eukaryota}
            ( undef, $psname_short ) = split ' : ', $ps_name;
        }
        else {                      #{Eukaryota}
            $psname_short = $ps_name;
        }

        # update map with new phylostrata (shorter phylogeny)
        my $ps_new;
        if ( exists $param_href->{ps}->{$ps} ) {
            $ps_new = $param_href->{ps}->{$ps};

            #say "LINE:$.\tPS_INFILE:$ps\tPS_NEW:$ps_new";
            $ps = $ps_new;
        }

        # update map with new tax_id (shorter phylogeny)
        my $ti_new;
        if ( exists $param_href->{ti}->{$ti} ) {
            $ti_new = $param_href->{ti}->{$ti};

            #say "LINE:$.\tTI_INFILE:$ti\tTI_NEW:$ti_new";
            $ti = $ti_new;
        }

        # update map with new phylostrata name (shorter phylogeny)
        my $psname_new;
        if ( exists $param_href->{psname}->{$psname_short} ) {
            $psname_new = $param_href->{psname}->{$psname_short};

            #say "LINE:$.\tPS_REAL_NAME:$psname_short\tPSNAME_NEW:$psname_new";
            $psname_short = $psname_new;
        }

        # print to tmp map file
        say {$tmp_fh} "$prot_id\t$ps\t$ti\t$psname_short";

    }    # end while

    # explicit close needed else it can break
    close $tmp_fh;

    my $map_block_cnt = 0;
  MAP: {
        # drop and recreate table where we are importing
        my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$map_tbl};
        my ( $success_drop, $res_drop ) = _http_exec_query( { query => $query_drop, %$param_href } );
        $log->debug("Action: table {$map_tbl} dropped successfully!") if $success_drop;
        if ( !$success_drop ) {
            $log->error("Error: dropping {$map_tbl} failed!");
            $map_block_cnt++;
            $log->logdie("Error: tried to drop {$map_tbl} 5 times with no success") if ( $map_block_cnt > 5 );
            redo MAP;
        }

        # wait a sec so it has time to execute
        sleep 1;

        # create table
        my $columns      = q{(prot_id String, ps UInt8, ti UInt32, psname String, date Date DEFAULT today())};
        my $engine       = q{ENGINE=MergeTree(date, prot_id, 8192)};
        my $query_create = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$map_tbl $columns $engine};
        my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
        $log->debug("Action: table $map_tbl created successfully!") if $success_create;
        if ( !$success_create ) {
            $log->error("Error: creating {$map_tbl} failed!");
            $map_block_cnt++;
            $log->logdie("Error: tried to create {$map_tbl} 5 times with no success") if ( $map_block_cnt > 5 );
            redo MAP;
        }

        # wait a sec so it has time to execute
        sleep 5;

        # import into $map_tbl
        my $import_query
          = qq{INSERT INTO $param_href->{database}.$map_tbl (prot_id, ps, ti, psname) FORMAT TabSeparated};
        my $import_cmd = qq{ cat $temp_map | clickhouse-client --stacktrace --query "$import_query"};
        my ( $stdout, $stderr, $exit ) = _capture_output( $import_cmd, $param_href );
        if ( $exit == 0 ) {
            $log->debug("Action: import to $param_href->{database}.$map_tbl success!");
        }
        else {
            $log->error("Error: {$import_cmd} failed: $stderr");
            $map_block_cnt++;
            $log->logdie("Error: tried to import to {$map_tbl} 5 times with no success") if ( $map_block_cnt > 5 );
            redo MAP;
        }

        # wait a sec so it has time to execute
        sleep 1;

        # check number of rows inserted
        my $query_cnt = qq{SELECT count() FROM $param_href->{database}.$map_tbl};
        my ( $success_cnt, $res_cnt ) = _http_exec_query( { query => $query_cnt, %$param_href } );
        $res_cnt =~ s/\n//g;    # remove trailing newline
        $log->info("Action: inserted $res_cnt rows into {$map_tbl}") if $success_cnt;
        $log->debug("Error: counting rows for $map_tbl failed!") unless $success_cnt;
        if ( !$success_cnt ) {
            $log->error("Error: counting rows for {$map_tbl} failed!");
            $map_block_cnt++;
            $log->logdie("Error: tried to count rows for {$map_tbl} 5 times with no success") if ( $map_block_cnt > 5 );
            redo MAP;
        }

    }    # end MAP block

    # unlink tmp map file
    #unlink $temp_map and $log->warn("Action: $temp_map unlinked");

    return $map_tbl;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastdb_stats
# Purpose    : import BLAST db stats created by AnalyzePhyloDb
# Returns    : names of stats_ps_tbl and stats_gen_tbl
# Parameters : infile and connection paramaters
# Throws     : croaks if wrong number of parameters
# Comments   : splits analyze file into 2 files (ps summary and genomes list)
# See Also   :
sub import_blastdb_stats {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_blastdb_stats() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $stats = $param_href->{stats} or $log->logcroak('no --stats=filename specified on command line!');
    $stats =~ s/\./_/g;    #for files that have dots in name
    my $stats_ps_tbl = path($stats)->basename;
    $stats_ps_tbl .= '_stats_ps';
    my $stats_genomes_tbl = path($stats)->basename;
    $stats_genomes_tbl .= '_stats_genomes';

    # create tmp file for phylostrata part of stats file
    my $tmp_ps = path( path($stats)->parent, $stats_ps_tbl );
    open( my $tmp_ps_fh, ">", $tmp_ps ) or $log->logdie("Error: can't open ps_stats $tmp_ps for writing:$!");

    # create tmp file for genomes part of stats file
    my $tmp_stats = path( path($stats)->parent, $stats_genomes_tbl );
    open( my $tmp_stats_fh, ">", $tmp_stats )
      or $log->logdie("Error: can't open genomes_stats $tmp_stats for writing:$!");

    # read and write stats file into 2 files
    _read_stats_file( { %{$param_href}, tmp_ps_fh => $tmp_ps_fh, tmp_stats_fh => $tmp_stats_fh } );

    # PART 1: tmp ps stats tbl
    # drop and recreate $stats_ps_tbl
    my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$stats_ps_tbl};
    my ( $success_drop, $res_drop ) = _http_exec_query( { query => $query_drop, %$param_href } );
    $log->error("Error: dropping $stats_ps_tbl failed!") unless $success_drop;
    $log->debug("Action: table $stats_ps_tbl dropped successfully!") if $success_drop;

    # wait a sec so it has time to execute
    sleep 1;

    # create table
    my $columns      = q{(ps UInt8,num_of_genomes UInt32, ti UInt32, date Date DEFAULT today())};
    my $engine       = q{ENGINE=MergeTree(date, (ps, ti), 8192)};
    my $query_create = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$stats_ps_tbl $columns $engine};
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Error: creating $stats_ps_tbl failed!") unless $success_create;
    $log->debug("Action: table $stats_ps_tbl created successfully!") if $success_create;

    # wait a sec so it has time to execute
    sleep 5;

    # import into $stats_ps_tbl
    my $import_query_ps
      = qq{INSERT INTO $param_href->{database}.$stats_ps_tbl (ps, num_of_genomes, ti) FORMAT TabSeparated};
    my $import_cmd_ps = qq{ <  $tmp_ps clickhouse-client --query "$import_query_ps"};
    my ( $stdout_ps, $stderr_ps, $exit_ps ) = _capture_output( $import_cmd_ps, $param_href );
    if ( $exit_ps == 0 ) {
        $log->debug("Action: import to $param_href->{database}.$stats_ps_tbl success!");
    }
    else {
        $log->error("Error: $import_cmd_ps failed: $stderr_ps");
    }

    # wait a sec so it has time to execute
    sleep 1;

    # check number of rows inserted
    my $query_cnt_ps = qq{SELECT count() FROM $param_href->{database}.$stats_ps_tbl};
    my ( $success_cnt_ps, $res_cnt_ps ) = _http_exec_query( { query => $query_cnt_ps, %$param_href } );
    $res_cnt_ps =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: counting rows for $stats_ps_tbl failed!") unless $success_cnt_ps;
    $log->info("Action: inserted $res_cnt_ps rows into {$stats_ps_tbl}") if $success_cnt_ps;

    # PART 2: real ps stats table
    my $query_drop2 = qq{DROP TABLE IF EXISTS $param_href->{database}.${stats_ps_tbl}2};
    my ( $success_drop2, $res_drop2 ) = _http_exec_query( { query => $query_drop2, %$param_href } );
    $log->error("Error: dropping ${stats_ps_tbl}2 failed!") unless $success_drop2;
    $log->debug("Action: table ${stats_ps_tbl}2 dropped successfully!") if $success_drop2;

    # wait a sec so it has time to execute
    sleep 1;

    # create table
    my $query_create2
      = qq{CREATE TABLE $param_href->{database}.${stats_ps_tbl}2 ENGINE=MergeTree (date, ps, 8192) AS SELECT DISTINCT ps, sum(num_of_genomes) AS num_of_genomes, ti, date FROM $param_href->{database}.$stats_ps_tbl GROUP BY ps, ti, date ORDER BY ps};
    my ( $success_create2, $res_create2 ) = _http_exec_query( { query => $query_create2, %$param_href } );
    $log->error("Error: creating ${stats_ps_tbl}2 failed!") unless $success_create2;
    $log->debug("Action: table ${stats_ps_tbl}2 created successfully!") if $success_create2;

    # wait a sec so it has time to execute
    sleep 1;

    # check number of rows inserted (now in aggregated table)
    my $query_cnt_ps2 = qq{SELECT count() FROM $param_href->{database}.${stats_ps_tbl}2};
    my ( $success_cnt_ps2, $res_cnt_ps2 ) = _http_exec_query( { query => $query_cnt_ps2, %$param_href } );
    $res_cnt_ps2 =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: counting rows for ${stats_ps_tbl}2 failed!") unless $success_cnt_ps2;
    $log->info("Action: inserted $res_cnt_ps2 rows into {${stats_ps_tbl}2}") if $success_cnt_ps2;

    # PART 3: DROP original stats table and rename stats2 table to this table
    # my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$stats_ps_tbl};
    my ( $success_drop_again, $res_drop_again ) = _http_exec_query( { query => $query_drop, %$param_href } );
    $log->error("Error: dropping $stats_ps_tbl failed!") unless $success_drop_again;
    $log->debug("Action: table $stats_ps_tbl dropped successfully!") if $success_drop_again;

    # wait a sec so it has time to execute
    sleep 1;

    # rename stats2 table to this table
    my $query_rename_stats = qq{RENAME TABLE $param_href->{database}.${stats_ps_tbl}2 TO $param_href->{database}.$stats_ps_tbl};
    my ( $success_rename, $res_rename ) = _http_exec_query( { query => $query_rename_stats, %$param_href } );
    $res_rename =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: renaming ${stats_ps_tbl}2 to $stats_ps_tbl failed!") unless $success_rename;
    $log->info("Action: renamed ${stats_ps_tbl}2 to $stats_ps_tbl") if $success_rename;

    # PART 3: SECOND table
    # drop and recreate $stats_genomes_tbl
    my $query_drop_gen = qq{DROP TABLE IF EXISTS $param_href->{database}.$stats_genomes_tbl};
    my ( $success_drop_gen, $res_drop_gen ) = _http_exec_query( { query => $query_drop_gen, %$param_href } );
    $log->error("Error: dropping $stats_genomes_tbl failed!") unless $success_drop_gen;
    $log->debug("Action: table $stats_genomes_tbl dropped successfully!") if $success_drop_gen;

    # wait a sec so it has time to execute
    sleep 1;

    # create table
    my $columns_gen = q{(ps UInt8, psti UInt32, num_of_genes UInt32, ti UInt32, date Date DEFAULT today())};
    my $engine_gen  = q{ENGINE=MergeTree(date, (ps, ti), 8192)};
    my $query_create_gen
      = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$stats_genomes_tbl $columns_gen $engine_gen};
    my ( $success_create_gen, $res_create_gen ) = _http_exec_query( { query => $query_create_gen, %$param_href } );
    $log->error("Error: creating $stats_genomes_tbl failed!") unless $success_create_gen;
    $log->debug("Action: table $stats_genomes_tbl created successfully!") if $success_create_gen;

    # wait a sec so it has time to execute
    sleep 1;

    # import into $stats_genomes_tbl
    my $import_query_gen
      = qq{INSERT INTO $param_href->{database}.$stats_genomes_tbl (ps, psti, num_of_genes, ti) FORMAT TabSeparated};
    my $import_cmd_gen = qq{ <  $tmp_stats clickhouse-client --query "$import_query_gen"};
    my ( $stdout_gen, $stderr_gen, $exit_gen ) = _capture_output( $import_cmd_gen, $param_href );
    if ( $exit_gen == 0 ) {
        $log->debug("Action: import to $param_href->{database}.$stats_genomes_tbl success!");
    }
    else {
        $log->error("Error: $import_cmd_gen failed: $stderr_ps");
    }

    # wait a sec so it has time to execute
    sleep 1;

    # check number of rows inserted
    my $query_cnt_gen = qq{SELECT count() FROM $param_href->{database}.$stats_genomes_tbl};
    my ( $success_cnt_gen, $res_cnt_gen ) = _http_exec_query( { query => $query_cnt_gen, %$param_href } );
    $res_cnt_gen =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: counting rows for $stats_genomes_tbl failed!") unless $success_cnt_gen;
    $log->info("Action: inserted $res_cnt_gen rows into {$stats_genomes_tbl}") if $success_cnt_gen;

    # unlink tmp stats files
    #unlink $tmp_ps and $log->warn("Action: $tmp_ps unlinked");
    #unlink $tmp_stats and $log->warn("Action: $tmp_stats unlinked");

    return $stats_ps_tbl, $stats_genomes_tbl;
}

### INTERNAL UTILITY ###
# Usage      : _read_stats_file( { %{$param_href}, tmp_ps_fh => $tmp_ps_fh, tmp_stats_fh => $tmp_stats_fh  } );
# Purpose    : to read stats file and import it to database
# Returns    : nothing
# Parameters :
# Throws     : croaks if wrong number of parameters
# Comments   : part of --mode=import_blastdb_stats
# See Also   : --mode=import_blastdb_stats
sub _read_stats_file {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_read_stats_file() needs a $param_href') unless @_ == 1;
    my ($p_href) = @_;

    # read and write to files
    open( my $stats_fh, "<", $p_href->{stats} )
      or $log->logdie("Error: can't open file $p_href->{stats} for reading:$!");
    while (<$stats_fh>) {
        chomp;

        # if ps then summary line
        if (m/ps/) {
            my ( undef, $ps, $num_of_genomes, $ti, ) = split "\t", $_;

            # update phylostrata with new phylostrata (shorter phylogeny)
            my $ps_new;
            if ( exists $p_href->{ps}->{$ps} ) {
                $ps_new = $p_href->{ps}->{$ps};

                #say "LINE:$.\tPS_INFILE:$ps\tPS_NEW:$ps_new";
                $ps = $ps_new;
            }

            # update psti with new tax_id (shorter phylogeny)
            my $ti_new;
            if ( exists $p_href->{ti}->{$ti} ) {
                $ti_new = $p_href->{ti}->{$ti};

                #say "LINE:$.\tTI_INFILE:$ti\tTI_NEW:$ti_new";
                $ti = $ti_new;
            }

            # write to tmp_stats file
            say { $p_href->{tmp_ps_fh} } "$ps\t$num_of_genomes\t$ti";
        }

        # else normal genome in phylostrata line
        else {
            my ( $ps2, $psti, $num_of_genes, $ti2 ) = split "\t", $_;

            # update phylostrata with new phylostrata (shorter phylogeny)
            my $ps_new2;
            if ( exists $p_href->{ps}->{$ps2} ) {
                $ps_new2 = $p_href->{ps}->{$ps2};

                #say "LINE:$.\tPS_INFILE:$ps2\tPS_NEW:$ps_new2";
                $ps2 = $ps_new2;
            }

            # update psti with new tax_id (shorter phylogeny)
            my $psti_new;
            if ( exists $p_href->{ti}->{$psti} ) {
                $psti_new = $p_href->{ti}->{$psti};

                #say "LINE:$.\tTI_INFILE:$psti\tTI_NEW:$psti_new";
                $psti = $psti_new;
            }

            # print to file
            say { $p_href->{tmp_stats_fh} } "$ps2\t$psti\t$num_of_genes\t$ti2";
        }
    }    # end while reading stats file

    # explicit close needed else it can break
    close $p_href->{tmp_ps_fh};
    close $p_href->{tmp_stats_fh};

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_names
# Purpose    : loads names file to ClickHouse
# Returns    : $names_tbl
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : new format
# See Also   :
sub import_names {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_names() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $names = $param_href->{names} or $log->logcroak('no $names specified on command line!');
    my $names_tbl = path($names)->basename;
    $names_tbl =~ s/\./_/g;    # for files that have dots in name
    $names_tbl =~ s/_gz//g;    # for compressed files

    # get new handle
    my $ch = _get_ch($param_href);

    # create names table
    my $create_names = qq{
    CREATE TABLE $names_tbl (
    ti UInt32,
    species_name String,
    name_syn String,
    name_type String,
    date Date  DEFAULT today() )
    ENGINE=MergeTree (date, ti, 8192) };

    _create_table_ch( { table_name => $names_tbl, ch => $ch, query => $create_names, %$param_href } );
    $log->trace("Report: $create_names");

    # import into table (in ClickHouse) from gziped file (needs pigz)
    my $import_query
      = qq{INSERT INTO $param_href->{database}.$names_tbl (ti, species_name, name_syn, name_type) FORMAT TabSeparated};
    my $import_cmd = qq{ pigz -c -d $names | clickhouse-client --query "$import_query"};
    _import_into_table_ch( { import_cmd => $import_cmd, table_name => $names_tbl, %$param_href } );

    # check number of rows inserted
    my $row_cnt = _get_row_cnt_ch( { ch => $ch, table_name => $names_tbl, %$param_href } );

    # drop unnecessary columns
    my @col_to_drop = (qw(name_syn name_type));
    _drop_columns_ch( { ch => $ch, table_name => $names_tbl, col => \@col_to_drop, %$param_href } );

    return $names_tbl;
}


### INTERFACE SUB ###
# Usage      : --mode=blastout_uniq
# Purpose    : creates unique blastout table for analysis (essence of blastout file) and report_gene_hits_per_species_tbl
# Returns    : nothing
# Parameters : it needs blastout_tbl
# Throws     : croaks if wrong number of parameters
# Comments   : runs in around half a hour
# See Also   :
sub blastout_uniq_orig {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('blastout_uniq() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # get table name
    my $blastout_uniq_tbl = "$param_href->{blastout_tbl}_uniq";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blastout_uniq_tbl, ch => $ch, %{$param_href} } );

    # create blastout_uniq table ('\\\\d+' is '\\d+' in ClickHouse)
    my $blastout_uniq_query = qq{
    CREATE TABLE $param_href->{database}.$blastout_uniq_tbl
    ENGINE=MergeTree(date, (prot_id, ti), 8192)
    AS SELECT DISTINCT prot_id, toUInt32(extract(substring(blast_hit, 28,20), '\\\\d+')) AS ti, date
    FROM $param_href->{database}.$param_href->{blastout_tbl}
    };
    $log->trace("$blastout_uniq_query");

    eval { $ch->do($blastout_uniq_query) };
    $log->error("Error: creating {$param_href->{database}.$blastout_uniq_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blastout_uniq_tbl} created successfully") unless $@;

    # check number of rows inserted
    my $row_cnt = _get_row_cnt_ch( { ch => $ch, table_name => $blastout_uniq_tbl, %$param_href } );

    # PART 1: ADD phylostrata from map table
    my $blout_ps_tbl = "$param_href->{blastout_tbl}_uniq_ps";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_ps_tbl, ch => $ch, %{$param_href} } );

    # create table
    my $blout_uniq_ps_q = qq{
	CREATE TABLE  $param_href->{database}.$blout_ps_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id_bl AS prot_id, ti, date_bl AS date
	FROM (SELECT prot_id AS prot_id_bl, ti, date as date_bl FROM $param_href->{database}.$blastout_uniq_tbl)
	ALL INNER JOIN
	(SELECT prot_id, ps FROM $param_href->{database}.$param_href->{map_tbl})
	USING prot_id
	};
    $log->trace("$blout_uniq_ps_q");

    eval { $ch->do($blout_uniq_ps_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_ps_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_ps_tbl} created successfully") unless $@;

    # check number of rows inserted
    my $row_cnt_ps = _get_row_cnt_ch( { ch => $ch, table_name => "$blout_ps_tbl", %$param_href } );

    # PART 2: SELECT phylostrata-tax_id from right phylostrata based on analyze
    my $blout_analyze_tbl = "$param_href->{blastout_tbl}_uniq_analyze";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_analyze_tbl, ch => $ch, %{$param_href} } );

    my $blout_analyze_q = qq{
	CREATE TABLE $param_href->{database}.$blout_analyze_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id, ti_bl AS ti, date_bl AS date
	FROM (SELECT ps, prot_id, ti AS ti_bl, date as date_bl FROM $param_href->{database}.$blout_ps_tbl)
	ALL INNER JOIN
	(SELECT ps AS an_ps, ti  FROM $param_href->{database}.$param_href->{stats_gen_tbl})
	USING ti WHERE an_ps == ps
	};
    $log->trace("$blout_analyze_q");

    eval { $ch->do($blout_analyze_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_analyze_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_analyze_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$blout_analyze_tbl", %$param_href } );

    # PART 3: ADD species_name from names tbl
    my $blout_species_tbl = "$param_href->{blastout_tbl}_uniq_species";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_species_tbl, ch => $ch, %{$param_href} } );

    my $blout_species_q = qq{
	CREATE TABLE $param_href->{database}.$blout_species_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id, ti_bl AS ti, species_name, date_bl AS date
	FROM (SELECT ps, prot_id, ti AS ti_bl, date as date_bl FROM $param_href->{database}.$blout_analyze_tbl)
	ALL INNER JOIN
	(SELECT ti, species_name FROM $param_href->{database}.$param_href->{names_tbl})
	USING ti
	};
    $log->trace("$blout_species_q");

    eval { $ch->do($blout_species_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_species_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_species_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$blout_species_tbl", %$param_href } );

    # PART 4: CALCULATE count() as gene_hits_per_species
    my $report_gene_hits_per_species_tbl = "$param_href->{blastout_tbl}_report_per_species";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $report_gene_hits_per_species_tbl, ch => $ch, %{$param_href} } );

	# POSSIBLE BUG with this date column (if calculation happens around midnight) because of GROUP BY splitting on date
    my $report_gene_hits_per_species_q = qq{
	CREATE TABLE $param_href->{database}.$report_gene_hits_per_species_tbl
	ENGINE=MergeTree (date, (ps, ti), 8192)
	AS SELECT ps, ti, species_name, count() AS gene_hits_per_species, today() AS date
	FROM $param_href->{database}.$blout_species_tbl
	GROUP BY ps, ti, species_name, date
	ORDER BY ps, gene_hits_per_species DESC
	};
    $log->trace("$report_gene_hits_per_species_q");

    eval { $ch->do($report_gene_hits_per_species_q) };
    $log->error("Error: creating {$param_href->{database}.$report_gene_hits_per_species_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_gene_hits_per_species_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_gene_hits_per_species_tbl", %$param_href } );

    # PART 5: DROP EXTRA TABLES
    #_drop_table_only_ch( { table_name => $blastout_uniq_tbl, ch => $ch, %{$param_href} } );
    #_drop_table_only_ch( { table_name => $blout_ps_tbl,      ch => $ch, %{$param_href} } );
    #_drop_table_only_ch( { table_name => $blout_analyze_tbl, ch => $ch, %{$param_href} } );

	# PART 6: create genelist and insert it into table
	# create table that will hold updated info (with genelists)
    _drop_table_only_ch( { table_name => "${report_gene_hits_per_species_tbl}2", ch => $ch, %{$param_href} } );
	my $report_gene_hits_per_species_q2 = qq{
	CREATE TABLE $param_href->{database}.${report_gene_hits_per_species_tbl}2 (
    ps UInt8,
    ti UInt32,
    species_name String,
    gene_hits_per_species UInt64,
    genelist String,
    date Date  DEFAULT today() )
	ENGINE=MergeTree (date, (ps, ti), 8192)
	};
    $log->trace("$report_gene_hits_per_species_q2");

    eval { $ch->do($report_gene_hits_per_species_q2) };
    $log->error("Error: creating {$param_href->{database}.${report_gene_hits_per_species_tbl}2} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.${report_gene_hits_per_species_tbl}2} created successfully") unless $@;

	# open filehandle to scalar to write to it and import from it to database
	my $scalar;
	open (my $dbin_fh, ">", \$scalar) or $log->logdie("Error: can't open scalar variable for writing");

	#retrieve entire table to modify it and return back
	my $rows = $ch->select("SELECT ps, ti, species_name, gene_hits_per_species FROM $param_href->{database}.$report_gene_hits_per_species_tbl");
    foreach my $row (@$rows) {
		my ($ps, $ti, $species_name, $gene_hits) = @$row;
		print "TABLE$ps\t$ti\t$species_name\t{$gene_hits}\n";
		
		# get list of prot_ids associated with specific ti
		my $prot_ids =  $ch->select("SELECT prot_id FROM $param_href->{database}.$blout_species_tbl WHERE ti = $ti");
		my @genes;
		foreach my $prot_id (@$prot_ids) {
			push @genes, @$prot_id;
		}
		my $genelist = join ', ', @genes;

		# print to scalar filehandle
		print {$dbin_fh} "$ps\t$ti\t$species_name\t$gene_hits\t$genelist\n";
    }

	# import back to database
    my $import_query
      = qq{INSERT INTO $param_href->{database}.${report_gene_hits_per_species_tbl}2 (ps, ti, species_name, gene_hits_per_species, genelist) FORMAT TabSeparated VALUES ($scalar)};
    $ch->do($import_query);

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "${report_gene_hits_per_species_tbl}2", %$param_href } );

    return;
}

### INTERFACE SUB ###
# Usage      : --mode=blastout_uniq
# Purpose    : creates unique blastout table for analysis (essence of blastout file) and report_gene_hits_per_species_tbl
# Returns    : returns names of the blastout_uniq_tbl, blout_species_tbl and report_ps_tbl tables
# Parameters : it needs blastout_tbl
# Throws     : croaks if wrong number of parameters
# Comments   : runs in around half a hour
# See Also   :
sub blastout_uniq {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('blastout_uniq() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # PART 1: create blastout_uniq table with prot_id, ti and e_value
    # for large blast inputs use chunked version
    my $blastout_uniq_tbl = _blastout_uniq_start_chunked($param_href);

    # PART 2: ADD phylostrata from map table
    my $blout_ps_tbl = _blastout_uniq_map($param_href);

    # PART 3: SELECT phylostrata-tax_id from right phylostrata based on analyze
    my $blout_analyze_tbl = _blastout_uniq_analyze($param_href);

    # PART 4: ADD species_name from names tbl
    my $blout_species_tbl = _blastout_uniq_species($param_href);

    # PART 5: CALCULATE count() as gene_hits_per_species
    my $blout_report_tbl = _blastout_uniq_report($param_href);

    # PART 6: DROP EXTRA TABLES AND RENAME REPORT TABLE
    _drop_table_only_ch( { table_name => $param_href->{blastout_tbl},                ch => $ch, %{$param_href} } );
    _drop_table_only_ch( { table_name => "$param_href->{blastout_tbl}_uniq_ps",      ch => $ch, %{$param_href} } );
    _drop_table_only_ch( { table_name => "$param_href->{blastout_tbl}_uniq_analyze", ch => $ch, %{$param_href} } );

    return $blastout_uniq_tbl, $blout_species_tbl, "$param_href->{blastout_tbl}_report_per_species";
}


### CLASS METHOD/INSTANCE METHOD/INTERFACE SUB/INTERNAL UTILITY ###
# Usage      : my $blastout_uniq_tbl = _blastout_uniq_start( $param_href );
# Purpose    : to create blastout_uniq table with prot_id, ti and e_value
# Returns    : $blastout_uniq_tbl
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_start {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_start() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # get table name
    my $blastout_uniq_tbl = "$param_href->{blastout_tbl}_uniq";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blastout_uniq_tbl, ch => $ch, %{$param_href} } );

    # wait a sec so it has time to execute
    sleep 1;

    # create blastout_uniq table ('\\\\d+' is '\\d+' in ClickHouse)
    my $blastout_uniq_query = qq{
    CREATE TABLE $param_href->{database}.$blastout_uniq_tbl
    ENGINE=MergeTree(date, (prot_id, ti), 8192)
    AS SELECT DISTINCT prot_id, toUInt32(extract(substring(blast_hit, 28,20), '\\\\d+')) AS ti, min(e_value) AS e_value,date
    FROM $param_href->{database}.$param_href->{blastout_tbl}
    GROUP BY prot_id, ti, date
    };
    $log->trace("$blastout_uniq_query");

    eval { $ch->do($blastout_uniq_query) };
    $log->logdie("Error: creating {$param_href->{database}.$blastout_uniq_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blastout_uniq_tbl} created successfully") unless $@;

    # wait a sec so it has time to execute
    sleep 1;

    # check number of rows inserted
    my $row_cnt = _get_row_cnt_ch( { ch => $ch, table_name => $blastout_uniq_tbl, %$param_href } );

    return $blastout_uniq_tbl;
}


### CLASS METHOD/INSTANCE METHOD/INTERFACE SUB/INTERNAL UTILITY ###
# Usage      : my $blastout_uniq_tbl = _blastout_uniq_start_chunked( $param_href );
# Purpose    : to create blastout_uniq table with prot_id, ti and e_value
# Returns    : $blastout_uniq_tbl
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_start_chunked {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_start_chunked() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # get table name
    my $blastout_uniq_tbl = "$param_href->{blastout_tbl}_uniq";

    # create blastout_uniq table
    my $create_uniq_q = qq{
    CREATE TABLE $blastout_uniq_tbl (
    prot_id String,
    ti UInt32,
    e_value Float64,
    date Date  DEFAULT today() )
    ENGINE=MergeTree (date, (prot_id, ti), 8192) };

    _create_table_ch( { table_name => $blastout_uniq_tbl, ch => $ch, query => $create_uniq_q, %$param_href } );
    $log->trace("Report: $create_uniq_q");

    # retrieve all prot_ids to iterate on them
    my $select_prot_id_aref
      = $ch->select("SELECT DISTINCT prot_id FROM $param_href->{database}.$param_href->{blastout_tbl}");
    my @prot_id = map { $_->[0] } @{$select_prot_id_aref};
    my $prot_id_cnt = @prot_id;

    # iterate on prot_ids and insert into uniq_tbl
    foreach my $prot_id (@prot_id) {
        my $insert_uniq_q = qq{
        INSERT INTO $param_href->{database}.$blastout_uniq_tbl (prot_id, ti, e_value)
        SELECT DISTINCT prot_id, toUInt32(extract(substring(blast_hit, 28,20), '\\\\d+')) AS ti, min(e_value) AS e_value
        FROM $param_href->{database}.$param_href->{blastout_tbl}
        WHERE prot_id = '$prot_id'
        GROUP BY prot_id, ti
        };
        #$log->trace("$insert_uniq_q");
        eval { $ch->do($insert_uniq_q); };
        $log->logdie("Error: inserting {$param_href->{database}.$blastout_uniq_tbl} failed for prot_id:$prot_id: $@")
          if $@;
        #$log->trace("Action: {$param_href->{database}.$blastout_uniq_tbl} inserted for prot_id:$prot_id") unless $@;
    }
    $log->info("Action: {$param_href->{database}.$blastout_uniq_tbl} inserted with $prot_id_cnt prot_ids");

    # check number of rows inserted
    my $row_cnt = _get_row_cnt_ch( { ch => $ch, table_name => $blastout_uniq_tbl, %$param_href } );

    return $blastout_uniq_tbl;
}

### INTERNAL UTILITY ###
# Usage      : my $blout_ps_tbl = _blastout_uniq_map( $param_href );
# Purpose    : part I of blastout_uniq()
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_map {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_map() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # PART 1: ADD phylostrata from map table
    my $blout_ps_tbl = "$param_href->{blastout_tbl}_uniq_ps";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_ps_tbl, ch => $ch, %{$param_href} } );

    # create table
    my $blout_uniq_ps_q = qq{
	CREATE TABLE  $param_href->{database}.$blout_ps_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id_bl AS prot_id, ti, date_bl AS date
	FROM (SELECT prot_id AS prot_id_bl, ti, date as date_bl FROM $param_href->{database}.$param_href->{blastout_tbl}_uniq)
	ALL INNER JOIN
	(SELECT prot_id, ps FROM $param_href->{database}.$param_href->{map_tbl})
	USING prot_id
	};
    $log->trace("$blout_uniq_ps_q");

    eval { $ch->do($blout_uniq_ps_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_ps_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_ps_tbl} created successfully") unless $@;

    # check number of rows inserted
    my $row_cnt_ps = _get_row_cnt_ch( { ch => $ch, table_name => "$blout_ps_tbl", %$param_href } );

    return $blout_ps_tbl;
}


### INTERNAL UTILITY ###
# Usage      : my $blout_analyze_tbl = _blastout_uniq_analyze( $param_href );
# Purpose    : part II of blastout_uniq()
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_analyze {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_analyze() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

     # PART 2: SELECT phylostrata-tax_id from right phylostrata based on analyze
    my $blout_analyze_tbl = "$param_href->{blastout_tbl}_uniq_analyze";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_analyze_tbl, ch => $ch, %{$param_href} } );

    my $blout_analyze_q = qq{
	CREATE TABLE $param_href->{database}.$blout_analyze_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id, ti_bl AS ti, date_bl AS date
	FROM (SELECT ps, prot_id, ti AS ti_bl, date as date_bl FROM $param_href->{database}.$param_href->{blastout_tbl}_uniq_ps)
	ALL INNER JOIN
	(SELECT ps AS an_ps, ti  FROM $param_href->{database}.$param_href->{stats_gen_tbl})
	USING ti WHERE an_ps == ps
	};
    $log->trace("$blout_analyze_q");

    eval { $ch->do($blout_analyze_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_analyze_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_analyze_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$blout_analyze_tbl", %$param_href } );

    return $blout_analyze_tbl;
}


### INTERNAL UTILITY ###
# Usage      : my $blout_species_tbl = _blastout_uniq_species( $param_href );
# Purpose    : part III of blastout_uniq();
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_species {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_species() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # PART 3: ADD species_name from names tbl
    my $blout_species_tbl = "$param_href->{blastout_tbl}_uniq_species";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $blout_species_tbl, ch => $ch, %{$param_href} } );

    my $blout_species_q = qq{
	CREATE TABLE $param_href->{database}.$blout_species_tbl
	ENGINE=MergeTree (date, (ps, prot_id, ti), 8192)
	AS SELECT ps, prot_id, ti_bl AS ti, species_name, date_bl AS date
	FROM (SELECT ps, prot_id, ti AS ti_bl, date as date_bl FROM $param_href->{database}.$param_href->{blastout_tbl}_uniq_analyze)
	ALL INNER JOIN
	(SELECT ti, species_name FROM $param_href->{database}.$param_href->{names_tbl})
	USING ti
	};
    $log->trace("$blout_species_q");

    eval { $ch->do($blout_species_q) };
    $log->error("Error: creating {$param_href->{database}.$blout_species_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$blout_species_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$blout_species_tbl", %$param_href } );

    return $blout_species_tbl;
}


### CLASS METHOD/INSTANCE METHOD/INTERFACE SUB/INTERNAL UTILITY ###
# Usage      : my $blout_report_tbl = _blastout_uniq_report( $param_href );
# Purpose    : part IV of blastout_uniq()
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_report_old {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_report() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # PART 4: CALCULATE count() as gene_hits_per_species
    my $report_gene_hits_per_species_tbl = "$param_href->{blastout_tbl}_report_per_species";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $report_gene_hits_per_species_tbl, ch => $ch, %{$param_href} } );

    # POSSIBLE BUG with this date column (if calculation happens around midnight) because of GROUP BY splitting on date
    my $report_gene_hits_per_species_q = qq{
	CREATE TABLE $param_href->{database}.$report_gene_hits_per_species_tbl
	ENGINE=MergeTree (date, (ps, ti), 8192)
	AS SELECT ps, ti, species_name, count() AS gene_hits_per_species, today() AS date
	FROM $param_href->{database}.$param_href->{blastout_tbl}_uniq_species
	GROUP BY ps, ti, species_name, date
	ORDER BY ps, gene_hits_per_species DESC
	};
    $log->trace("$report_gene_hits_per_species_q");

    eval { $ch->do($report_gene_hits_per_species_q) };
    $log->error("Error: creating {$param_href->{database}.$report_gene_hits_per_species_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_gene_hits_per_species_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_gene_hits_per_species_tbl", %$param_href } );

    return $report_gene_hits_per_species_tbl;
}


### CLASS METHOD/INSTANCE METHOD/INTERFACE SUB/INTERNAL UTILITY ###
# Usage      : my $blout_report_tbl = _blastout_uniq_report( $param_href );
# Purpose    : part IV of blastout_uniq()
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _blastout_uniq_report {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_blastout_uniq_report() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # PART 4: CALCULATE count() as gene_hits_per_species
    my $report_gene_hits_per_species_tbl = "$param_href->{blastout_tbl}_report_per_species";

    # drop table if it exists
    _drop_table_only_ch( { table_name => $report_gene_hits_per_species_tbl, ch => $ch, %{$param_href} } );

    # POSSIBLE BUG with this date column (if calculation happens around midnight) because of GROUP BY splitting on date
    my $report_gene_hits_per_species_q = qq{
	CREATE TABLE $param_href->{database}.$report_gene_hits_per_species_tbl
	ENGINE=MergeTree (date, (ps, ti), 8192)
	AS SELECT ps, ti, species_name, count() AS gene_hits_per_species, 
	arrayStringConcat(groupArray(prot_id), ', ') as genelist, today() AS date
	FROM $param_href->{database}.$param_href->{blastout_tbl}_uniq_species
	GROUP BY ps, ti, species_name, date
	ORDER BY ps, gene_hits_per_species DESC
	};
    $log->trace("$report_gene_hits_per_species_q");

    eval { $ch->do($report_gene_hits_per_species_q) };
    $log->error("Error: creating {$param_href->{database}.$report_gene_hits_per_species_tbl} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_gene_hits_per_species_tbl} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_gene_hits_per_species_tbl", %$param_href } );

    return $report_gene_hits_per_species_tbl;
}


### INTERFACE SUB ###
# Usage      : --mode=bl_uniq_expanded
# Purpose    : expands genehits into types (how many hits per phylostratum) to find repeating and unique hits
# Returns    : name of the report_exp_tbl table
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : works with report table that --mode=blastout_uniq created
# See Also   : 
sub bl_uniq_expanded {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('bl_uniq_expanded() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # create table that will hold updated info (with genelists)
	_create_exp_tbl( $param_href );
    my $report_ps_tbl_exp = "$param_href->{report_ps_tbl}" . '_expanded';

    # build query to import back to database (needed later)
    my $import_query
      = qq{INSERT INTO $param_href->{database}.$report_ps_tbl_exp (ti, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10 ) VALUES};
    $log->trace("$import_query");

    # get phylostrata from REPORT_PER_PS table to iterate on phylostrata
    my $select_ps_q = qq{ SELECT DISTINCT ps FROM $param_href->{database}.$param_href->{report_ps_tbl} ORDER BY ps};
    my $ps_aref     = $ch->select($select_ps_q);
    my @ps          = map { $_->[0] } @{$ps_aref};
    $log->trace( 'Returned phylostrata: {', join( '}{', @ps ), '}' );

    # insert hits and genelists into database
    foreach my $ps (@ps) {

        #get gene_list from db
        my $select_gene_list_from_report_q = qq{
        SELECT DISTINCT ti, genelist
        FROM $param_href->{database}.$param_href->{report_ps_tbl}
        WHERE ps = $ps
        ORDER BY gene_hits_per_species
        };
        my $ti_genelist_aref = $ch->select($select_gene_list_from_report_q);
        my %ti_genelist_h = map { $_->[0], $_->[1] } @{$ti_genelist_aref};

        # get ti list sorted by gene_hits_per_species
        my @ti = map { $_->[0] } @{$ti_genelist_aref};

        # transform gene_list to array and push all arrays into single array
        my @full_genelist;
        foreach my $ti (@ti) {
            my @gene_list_a = split ",", $ti_genelist_h{$ti};
            $ti_genelist_h{$ti} = \@gene_list_a;
            push @full_genelist, @gene_list_a;
        }

        # get count of each prot_id
        my %gene_count;
        foreach my $prot_id (@full_genelist) {
            $gene_count{$prot_id}++;
        }

        # get unique count per tax_id
        my @arg_list;
        my $arg_to_insert_cnt = 0;
        foreach my $ti (@ti) {
            my @ti_genelist = @{ $ti_genelist_h{$ti} };
            my ( $ti_unique,     $ti2,  $ti3,  $ti4,  $ti5,  $ti6,  $ti7,  $ti8,  $ti9,  $ti10 )  = (0) x 11;
            my ( $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g ) = ('') x 11;

            # do the calculation here (tabulated ternary) 10 and 10+hits go to hits10
            foreach my $prot_id (@ti_genelist) {
                    $gene_count{$prot_id} == 1 ? do { $ti_unique++; $ti_uniq_genes .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 2 ? do { $ti2++;       $ti2g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 3 ? do { $ti3++;       $ti3g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 4 ? do { $ti4++;       $ti4g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 5 ? do { $ti5++;       $ti5g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 6 ? do { $ti6++;       $ti6g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 7 ? do { $ti7++;       $ti7g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 8 ? do { $ti8++;       $ti8g          .= ',' . $prot_id; }
                  : $gene_count{$prot_id} == 9 ? do { $ti9++;       $ti9g          .= ',' . $prot_id; }
                  :                              do { $ti10++;      $ti10g         .= ',' . $prot_id; };
            }

            # remove comma at start
            foreach my $genelist ( $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g ) {
                $genelist =~ s/\A,(.+)\z/$1/;
            }

            # insert into db
            push @arg_list,
              [ $ti, $ti_unique, $ti2, $ti3, $ti4, $ti5, $ti6, $ti7, $ti8, $ti9, $ti10,
                $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g
              ];
            $arg_to_insert_cnt++;

            # insert in small chunks (else Perl memory grows)
            if ( $arg_to_insert_cnt >= 1000 ) {
                my $ch2 = _get_ch($param_href);

                eval { $ch2->do( $import_query, @arg_list ) };
                $log->error("Error: inserting into {$param_href->{database}.$report_ps_tbl_exp} failed: $@")
                  if $@;
                $log->trace(
                    "Action: {$param_href->{database}.$report_ps_tbl_exp} inserted successfully with {$arg_to_insert_cnt} records"
                ) unless $@;

                # back to empty for another chunk
                $arg_to_insert_cnt = 0;
                @arg_list          = ();
            }

            #say "TI:$ti\tuniq:$ti_unique\tti2:$ti2\tti3:$ti3\tti4:$ti4\tti5:$ti5\tti6:$ti6\tti7:$ti7\tti8:$ti8\tti9:$ti9\tti10:$ti10";
            #say  "TI:$ti\tuniq:$ti_uniq_genes\tti2:$ti2g\tti3:$ti3g\tti4:$ti4g\tti5:$ti5g\tti6:$ti6g\tti7:$ti7g\tti8:$ti8g\tti9:$ti9g\tti10:$ti10g";
        }

        # import all remaining calculated values into table
        # get new handle
        my $ch3 = _get_ch($param_href);

        eval { $ch3->do( $import_query, @arg_list ) };
        $log->error("Error: inserting into {$param_href->{database}.$report_ps_tbl_exp} failed: $@")
          if $@;
        $log->trace("Action: {$param_href->{database}.$report_ps_tbl_exp} inserted successfully with {$arg_to_insert_cnt} records")
          unless $@;

        $log->debug("Report: inserted ps $ps");
    }    # end foreach ps

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_ps_tbl_exp", %$param_href } );

    # PART 2: JOIN report and report_expanded table
    # create final table that will hold updated info (with unique hits and lists)
    my $report_ps_tbl_exp2 = "$param_href->{report_ps_tbl}" . '_expanded2';
    _drop_table_only_ch( { table_name => $report_ps_tbl_exp2, ch => $ch, %{$param_href} } );

    # join expanded table and original report table to get all columns together
    my $report_ps_tbl_exp_q2 = qq{
    CREATE TABLE $param_href->{database}.$report_ps_tbl_exp2
    ENGINE=MergeTree (date, (ps, ti), 8192)
    AS SELECT ps, ti_exp AS ti, species_name, gene_hits_per_species, genelist, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10, date_bl AS date
    FROM (SELECT ti AS ti_exp, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10, date AS date_bl
    FROM $param_href->{database}.$report_ps_tbl_exp)
    ALL INNER JOIN
    (SELECT ps, ti, species_name, gene_hits_per_species, genelist FROM $param_href->{database}.$param_href->{report_ps_tbl})
    USING ti
    };
    $log->trace("$report_ps_tbl_exp_q2");

    eval { $ch->do($report_ps_tbl_exp_q2) };
    $log->error("Error: creating {$param_href->{database}.$report_ps_tbl_exp2} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_ps_tbl_exp2} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_ps_tbl_exp2", %$param_href } );

    # PART 3: DROP EXTRA TABLES AND RENAME REPORT TABLE
    _drop_table_only_ch( { table_name => $report_ps_tbl_exp, ch => $ch, %{$param_href} } );

    # rename report2 table to report table
    my $rename_report_tbl_q
      = qq{RENAME TABLE $param_href->{database}.$report_ps_tbl_exp2 TO $param_href->{database}.$report_ps_tbl_exp};
    eval { $ch->do($rename_report_tbl_q) };
    $log->error("Error: renaming {$param_href->{database}.$report_ps_tbl_exp2} failed: $@") if $@;
    $log->info(
        "Action: {$param_href->{database}.$report_ps_tbl_exp2} renamed to {$param_href->{database}.$report_ps_tbl_exp} successfully"
    ) unless $@;

    return $report_ps_tbl_exp;
}


### INTERNAL UTILITY ###
# Usage      : _create_exp_tbl( $param_href );
# Purpose    : creates 
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _create_exp_tbl {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_create_exp_tbl() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # create table that will hold updated info (with genelists)
    my $report_ps_tbl_exp = "$param_href->{report_ps_tbl}" . '_expanded';
    _drop_table_only_ch( { table_name => $report_ps_tbl_exp, ch => $ch, %{$param_href} } );
    my $report_ps_tbl_exp_q = qq{
    CREATE TABLE $param_href->{database}.$report_ps_tbl_exp (
    ps UInt8,
    ti UInt32,
    species_name String,
    gene_hits_per_species UInt64,
    genelist String,
    hits1 UInt32,
    hits2 UInt32,
    hits3 UInt32,
    hits4 UInt32,
    hits5 UInt32,
    hits6 UInt32,
    hits7 UInt32,
    hits8 UInt32,
    hits9 UInt32,
    hits10 UInt32,
    list1 String,
    list2 String,
    list3 String,
    list4 String,
    list5 String,
    list6 String,
    list7 String,
    list8 String,
    list9 String,
    list10 String,
    date Date  DEFAULT today()
    )ENGINE=MergeTree (date, (ps, ti), 8192)
    };
    $log->trace("$report_ps_tbl_exp_q");

    eval { $ch->do($report_ps_tbl_exp_q) };
    $log->error("Error: creating {$param_href->{database}.$report_ps_tbl_exp} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_ps_tbl_exp} created successfully") unless $@;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=bl_uniq_exp_iter
# Purpose    : expands genehits into types (how many hits per phylostratum) to find repeating and unique hits
# Returns    : name of the report_exp_tbl table
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : works with report table that --mode=blastout_uniq created
# See Also   : 
sub bl_uniq_exp_iter {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('bl_uniq_exp_iter() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # create table that will hold updated info (with genelists)
    _create_exp_tbl($param_href);
    my $report_ps_tbl_exp = "$param_href->{report_ps_tbl}" . '_expanded';

    # build query to import back to database (needed later)
    my $import_query
      = qq{INSERT INTO $param_href->{database}.$report_ps_tbl_exp (ti, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10 ) VALUES};
    $log->trace("$import_query");

    # get phylostrata from REPORT_PER_PS table to iterate on phylostrata
    my $select_ps_q = qq{ SELECT DISTINCT ps FROM $param_href->{database}.$param_href->{report_ps_tbl} ORDER BY ps};
    my $ps_aref     = $ch->select($select_ps_q);
    my @ps          = map { $_->[0] } @{$ps_aref};
    $log->trace( 'Returned phylostrata: {', join( '}{', @ps ), '}' );

    # iterate on phylostrata (compare genelists inside phylostrata)
    foreach my $ps (@ps) {

        #get ti_list from db per ps sorted by gene_hits_per_species
        my $select_ti_list_from_report_q = qq{
        SELECT DISTINCT ti
        FROM $param_href->{database}.$param_href->{report_ps_tbl}
        WHERE ps = $ps
        ORDER BY gene_hits_per_species
        };
        my $ti_aref = $ch->select($select_ti_list_from_report_q);
        my @ti = map { $_->[0] } @{$ti_aref};

        # count prot_ids ans store into hash to be used later
        my %prot_id_seen;
        foreach my $ti (@ti) {

            #get gene_list from db
            my $select_genelist_from_report_q = qq{
            SELECT genelist
            FROM $param_href->{database}.$param_href->{report_ps_tbl}
            WHERE ps = $ps AND ti = $ti
            };
            my $genelist_aref = $ch->select($select_genelist_from_report_q);
            my @genelist_one  = @{ $genelist_aref->[0] };
            my @genelist_exp  = split ",", $genelist_one[0];

            # get count of each prot_id
            foreach my $prot_id (@genelist_exp) {
                $prot_id_seen{$prot_id}++;
            }
        }

        # now extract information from stored hash to label prot_ids as unique
        # get unique count per tax_id
        my @arg_list;
        my $arg_to_insert_cnt = 0;
        foreach my $ti (@ti) {

            #get gene_list from db
            my $select_genelist_from_report_q = qq{
            SELECT genelist
            FROM $param_href->{database}.$param_href->{report_ps_tbl}
            WHERE ps = $ps AND ti = $ti
            };
            my $genelist_aref = $ch->select($select_genelist_from_report_q);
            my @genelist_one  = @{ $genelist_aref->[0] };
            my @genelist_exp  = split ",", $genelist_one[0];

            my ( $ti_unique,     $ti2,  $ti3,  $ti4,  $ti5,  $ti6,  $ti7,  $ti8,  $ti9,  $ti10 )  = (0) x 11;
            my ( $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g ) = ('') x 11;

            # do the calculation here (tabulated ternary) 10 and 10+hits go to hits10
            foreach my $prot_id (@genelist_exp) {
                    $prot_id_seen{$prot_id} == 1 ? do { $ti_unique++; $ti_uniq_genes .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 2 ? do { $ti2++;       $ti2g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 3 ? do { $ti3++;       $ti3g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 4 ? do { $ti4++;       $ti4g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 5 ? do { $ti5++;       $ti5g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 6 ? do { $ti6++;       $ti6g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 7 ? do { $ti7++;       $ti7g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 8 ? do { $ti8++;       $ti8g          .= ',' . $prot_id; }
                  : $prot_id_seen{$prot_id} == 9 ? do { $ti9++;       $ti9g          .= ',' . $prot_id; }
                  :                                do { $ti10++;      $ti10g         .= ',' . $prot_id; };
            }

            # remove comma at start
            foreach my $genelist ( $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g ) {
                $genelist =~ s/\A,(.+)\z/$1/;
            }

            #say "TI_numbe:$ti\tuniq:$ti_unique\tti2:$ti2\tti3:$ti3\tti4:$ti4\tti5:$ti5\tti6:$ti6\tti7:$ti7\tti8:$ti8\tti9:$ti9\tti10:$ti10";
            #say "TI_genes:$ti\tuniq:$ti_uniq_genes\tti2:$ti2g\tti3:$ti3g\tti4:$ti4g\tti5:$ti5g\tti6:$ti6g\tti7:$ti7g\tti8:$ti8g\tti9:$ti9g\tti10:$ti10g";

            # insert into db
            push @arg_list,
              [ $ti, $ti_unique, $ti2, $ti3, $ti4, $ti5, $ti6, $ti7, $ti8, $ti9, $ti10,
                $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g
              ];
            $arg_to_insert_cnt++;

            # insert in small chunks (else Perl memory grows)
            if ( $arg_to_insert_cnt >= 1000 ) {
                my $ch2 = _get_ch($param_href);

                eval { $ch2->do( $import_query, @arg_list ) };
                my $arg_list_dump = sprintf("%s", Dumper \@arg_list);
                $log->error("Error: inserting into {$param_href->{database}.$report_ps_tbl_exp} failed: $@ $import_query ARG_LIST:$arg_list_dump")
                  if $@;
                $log->trace(
                    "Action: {$param_href->{database}.$report_ps_tbl_exp} inserted successfully with {$arg_to_insert_cnt} records"
                ) unless $@;

                # back to empty for another chunk
                $arg_to_insert_cnt = 0;
                @arg_list          = ();
            }

        }    # end second foreach ti

        # import all remaining calculated values into table
        # get new handle
        my $ch3 = _get_ch($param_href);

        eval { $ch3->do( $import_query, @arg_list ) };
        $log->error("Error: inserting into {$param_href->{database}.$report_ps_tbl_exp} failed: $@")
          if $@;
        $log->trace(
            "Action: {$param_href->{database}.$report_ps_tbl_exp} inserted successfully with {$arg_to_insert_cnt} records"
        ) unless $@;

        $log->debug("Report: inserted ps $ps");
    }    # end foreach ps

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_ps_tbl_exp", %$param_href } );

    # PART 2: JOIN report and report_expanded table
    # create final table that will hold updated info (with unique hits and lists)
    my $report_ps_tbl_exp2 = "$param_href->{report_ps_tbl}" . '_expanded2';
    _drop_table_only_ch( { table_name => $report_ps_tbl_exp2, ch => $ch, %{$param_href} } );

    # join expanded table and original report table to get all columns together
    my $report_ps_tbl_exp_q2 = qq{
    CREATE TABLE $param_href->{database}.$report_ps_tbl_exp2
    ENGINE=MergeTree (date, (ps, ti), 8192)
    AS SELECT ps, ti_exp AS ti, species_name, gene_hits_per_species, genelist, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10, date_bl AS date
    FROM (SELECT ti AS ti_exp, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10, list1, list2, list3, list4, list5, list6, list7, list8, list9, list10, date AS date_bl
    FROM $param_href->{database}.$report_ps_tbl_exp)
    ALL INNER JOIN
    (SELECT ps, ti, species_name, gene_hits_per_species, genelist FROM $param_href->{database}.$param_href->{report_ps_tbl})
    USING ti
    };
    $log->trace("$report_ps_tbl_exp_q2");

    eval { $ch->do($report_ps_tbl_exp_q2) };
    $log->error("Error: creating {$param_href->{database}.$report_ps_tbl_exp2} failed: $@") if $@;
    $log->info("Action: {$param_href->{database}.$report_ps_tbl_exp2} created successfully") unless $@;

    # check number of rows inserted
    _get_row_cnt_ch( { ch => $ch, table_name => "$report_ps_tbl_exp2", %$param_href } );

    # PART 3: DROP EXTRA TABLES AND RENAME REPORT TABLE
    _drop_table_only_ch( { table_name => $report_ps_tbl_exp, ch => $ch, %{$param_href} } );

    # rename report2 table to report table
    my $rename_report_tbl_q
      = qq{RENAME TABLE $param_href->{database}.$report_ps_tbl_exp2 TO $param_href->{database}.$report_ps_tbl_exp};
    eval { $ch->do($rename_report_tbl_q) };
    $log->error("Error: renaming {$param_href->{database}.$report_ps_tbl_exp2} failed: $@") if $@;
    $log->info(
        "Action: {$param_href->{database}.$report_ps_tbl_exp2} renamed to {$param_href->{database}.$report_ps_tbl_exp} successfully"
    ) unless $@;

    return $report_ps_tbl_exp;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastdb
# Purpose    : loads BLAST database to ClickHouse database from compressed file using named pipe
# Returns    : $param_href->{blastdb_tbl}
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : works on compressed file
# See Also   : 
sub import_blastdb {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_blastdb() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $blastdb = $param_href->{blastdb} or $log->logcroak('no $blastdb specified on command line!');
    my $blastdb_tbl = path($blastdb)->basename;
    $blastdb_tbl =~ s/\.gz//g;   # for compressed files
    $blastdb_tbl =~ s/\./_/g;    # for files that have dots in name
    $param_href->{blastdb_tbl} = $blastdb_tbl;
    my $out = path($blastdb)->parent;

    # get date for named pipe file naming
    my $now = DateTime::Tiny->now;
    my $date
      = $now->year . '_' . $now->month . '_' . $now->day . '_' . $now->hour . '_' . $now->minute . '_' . $now->second;

    # delete pipe if it exists
    my $load_file = path( $out, "blastdb_named_pipe_${date}" );    #file for LOAD DATA INFILE
    if ( -p $load_file ) {
        unlink $load_file and $log->trace("Action: named pipe $load_file removed!");
    }

    #make named pipe
    mkfifo( $load_file, 0666 ) or $log->logdie("Error: mkfifo $load_file failed: $!");

    # open blastdb compressed file for reading
    open my $blastdb_fh, "<:gzip", $blastdb or $log->logdie("Error: can't open gziped file $blastdb: $!");

    #start 2 processes (one for Perl-child and ClickHouse-parent)
    my $pid = fork;

    if ( !defined $pid ) {
        $log->logdie("Error: cannot fork: $!");
    }

    elsif ( $pid == 0 ) {

        # Child-client process
        $log->warn("Action: Perl-child-client starting...");

        # open named pipe for writing (gziped file --> named pipe)
        open my $blastdb_pipe_fh, "+<:encoding(ASCII)", $load_file or die $!;    #+< mode=read and write

        # define new block for reading blocks of fasta
        {
            local $/ = ">pgi";    #look in larger chunks between >gi (solo > found in header so can't use)
            local $.;             #gzip count
            my $out_cnt = 0;      #named pipe count

            # print to named pipe
          PIPE:
            while (<$blastdb_fh>) {
                chomp;

                #print $blastdb_pipe_fh "$_";
                #say '{', $_, '}';
                next PIPE if $_ eq '';    #first iteration is empty?

                # extract pgi, prot_name and fasta + fasta
                my ( $prot_id, $prot_name, $fasta ) = $_ =~ m{\A([^\t]+)\t([^\n]+)\n(.+)\z}smx;

                #pgi removed as record separator (return it back)
                $prot_id = 'pgi' . $prot_id;
                my ( $pgi, $ti ) = $prot_id =~ m{pgi\|(\d+)\|ti\|(\d+)\|pi\|(?:\d+)\|};

                # remove illegal chars from fasta and upercase it
                $fasta =~ s/\R//g;        #delete multiple newlines (all vertical and horizontal space)
                $fasta = uc $fasta;       #uppercase fasta
                $fasta =~ tr{A-Z}{}dc;    #delete all special characters (all not in A-Z)

                # print to pipe
                print {$blastdb_pipe_fh} "$prot_id\t$pgi\t$ti\t$prot_name\t$fasta\n";
                $out_cnt++;

                #progress tracker for blastdb file
                if ( $. % 1000000 == 0 ) {
                    $log->trace("$. lines processed!");
                }
            }
            my $blastdb_file_line_cnt = $. - 1;    #first line read empty (don't know why)
            $log->warn("Report: file $blastdb has $blastdb_file_line_cnt fasta records!");
            $log->warn("Action: file $load_file written with $out_cnt lines/fasta records!");
        }    #END block writing to pipe

        $log->warn("Action: Perl-child-client terminating :)");
        exit 0;
    }
    else {
        # ClickHouse-parent process
        $log->warn("Action: ClickHouse-parent process, waiting for child...");

        # SECOND PART: loading named pipe into db
        my $database = $param_href->{database} or $log->logcroak('no $database specified on command line!');

        # get new handle
        my $ch = _get_ch($param_href);

        # create blastdb table
        my $create_blastdb_q = qq{
        CREATE TABLE $blastdb_tbl (
        prot_id String,
        pgi String,
        ti UInt32,
        prot_name String,
        fasta String,
        date Date  DEFAULT today() )
        ENGINE=MergeTree (date, (ti, pgi, prot_name), 8192) };

        _create_table_ch( { table_name => $blastdb_tbl, ch => $ch, query => $create_blastdb_q, %$param_href } );
        $log->trace("Report: $create_blastdb_q");

        # import into table (in ClickHouse) from named pipe
        my $import_query
          = qq{INSERT INTO $param_href->{database}.$blastdb_tbl (prot_id, pgi, ti, prot_name, fasta) FORMAT TabSeparated};
        my $import_cmd = qq{ cat $load_file | clickhouse-client --query "$import_query"};
        _import_into_table_ch( { import_cmd => $import_cmd, table_name => $blastdb_tbl, %$param_href } );

        # check number of rows inserted
        my $row_cnt = _get_row_cnt_ch( { ch => $ch, table_name => $blastdb_tbl, %$param_href } );

        # communicate with child process
        waitpid $pid, 0;
    }
    $log->warn("ClickHouse-parent process end after child has finished");
    unlink $load_file and $log->warn("Action: named pipe $load_file removed!");

    return $param_href->{blastdb_tbl};
}


## INTERNAL UTILITY ###
# Usage      : my $clickhouse_handle = _get_ch( $param_href );
# Purpose    : gets a ClickHouse handle using http connection
# Returns    : ClickHouse handle to execute queries
# Parameters : database connection params
# Throws     : ClickHouse module errors and warnings
# Comments   : utility function to connect to Clickhouse
# See Also   : _http_exec_query (which uses HTTP::Tiny for similar purpose
sub _get_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_get_ch() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $user     = defined $param_href->{user}     ? $param_href->{user}     : 'default';
    my $password = defined $param_href->{password} ? $param_href->{password} : '';
    my $host     = defined $param_href->{host}     ? $param_href->{host}     : 'localhost';
    my $database = defined $param_href->{database} ? $param_href->{database} : 'default';
    my $port     = defined $param_href->{port}     ? $param_href->{port}     : 8123;

    my $ch = ClickHouse->new(
        host     => $host,
        port     => $port,
        database => $database,
        user     => $user,
        password => $password,
    );

	#print Dumper $ch;

    return $ch;
}


### INTERNAL UTILITY ###
# Usage      : _create_table_ch( { table_name => $table_info, ch => $ch, query => $create_query, drop => 1, %{$param_href} } );
# Purpose    : it drops and recreates table in ClickHouse
# Returns    : nothing
# Parameters : hash_ref of table_name, dbh and query
# Throws     : errors if it fails
# Comments   : 
# See Also   : 
sub _create_table_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_create_table_ch() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $table_name    = $param_href->{table_name} or $log->logcroak('no $table_name sent to _create_table_ch()!');
    my $ch            = $param_href->{ch}         or $log->logcroak('no $ch sent to _create_table_ch()!');
    my $create_query  = $param_href->{query}      or $log->logcroak('no $query sent to _create_table_ch()!');
    my $drop_decision = defined $param_href->{drop} ? $param_href->{drop} : 1;   # by default it drops table

	# drop table by default, else leave it
	if ($drop_decision == 1) {
        my $drop_query = qq{ DROP TABLE IF EXISTS $param_href->{database}.$table_name };
        eval { $ch->do($drop_query) };
        $log->error("Error: dropping {$param_href->{database}.$table_name} failed: $@") if $@;
        $log->trace("Action: {$param_href->{database}.$table_name} dropped successfully") unless $@;
	}
	else {
		$log->warn("Warn: leaving table {$param_href->{database}.$table_name} in database");
	}

	#create table in database specified in connection
    eval { $ch->do($create_query) };
    $log->error( "Error: creating {$param_href->{database}.$table_name} failed: $@" ) if $@;
    $log->info( "Action: {$param_href->{database}.$table_name} created successfully" ) unless $@;

    return;
}


### INTERNAL UTILITY ###
# Usage      : _drop_table_only_ch( { table_name => $table, ch => $ch, %{$param_href} } );
# Purpose    : it drops table in ClickHouse
# Returns    : nothing
# Parameters : hash_ref of table_name, dbh and query
# Throws     : errors if it fails
# Comments   :
# See Also   :
sub _drop_table_only_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_drop_table_only_ch() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $table_name = $param_href->{table_name} or $log->logcroak('no $table_name sent to _drop_table_only_ch()!');
    my $ch         = $param_href->{ch}         or $log->logcroak('no $ch sent to _drop_table_only_ch()!');

    # drop table in database specified in connection
    my $drop_query = qq{ DROP TABLE IF EXISTS $param_href->{database}.$table_name };
    eval { $ch->do($drop_query) };
    $log->error("Error: dropping {$param_href->{database}.$table_name} failed: $@") if $@;
    $log->trace("Action: {$param_href->{database}.$table_name} dropped successfully") unless $@;

    return;
}


### INTERNAL UTILITY ###
# Usage      : _import_into_table_ch( { import_cmd => $import_cmd, table_name => $tbl, %$param_href } );
# Purpose    : import into ClickHouse using command line client
# Returns    : nothing
# Parameters : command to import
# Throws     : croaks if wrong number of parameters
# Comments   : needs pigz for gzip archives
# See Also   : 
sub _import_into_table_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_into_table_ch() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;
    my $import_cmd   = $param_href->{import_cmd} or $log->logcroak('no $import_cmd sent to _import_into_table_ch()!');

	# import into table (in ClickHouse) from gziped file (needs pigz)
    my ( $stdout, $stderr, $exit ) = _capture_output( $import_cmd, $param_href );
    if ( $exit == 0 ) {
        $log->info("Action: import to {$param_href->{database}.$param_href->{table_name}} success");
    }
    else {
        $log->error("Error: $import_cmd failed: $stderr");
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : my $row_cnt = _get_row_cnt_ch( { ch -> $ch, table_name => $names_tbl, %$param_href } );
# Purpose    : to count number of rows in a table
# Returns    : number of rows
# Parameters : ClickHouse handle, table name and database name in params
# Throws     : croaks if wrong number of parameters
# Comments   : uses full column scan on shortest column to get row count
# See Also   :
sub _get_row_cnt_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_get_row_cnt_ch() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # check number of rows inserted
    my $query_cnt = qq{ SELECT count() FROM $param_href->{database}.$param_href->{table_name} };
    my $rows;
    eval { $rows = $param_href->{ch}->select($query_cnt); };
    my $row_cnt = $rows->[0][0];
    $log->error("Error: counting rows for {$param_href->{database}.$param_href->{table_name}} failed") if $@;
    $log->info("Report: table {$param_href->{database}.$param_href->{table_name}} contains $row_cnt rows") unless $@;

    return $row_cnt;
}


### INTERNAL UTILITY ###
# Usage      : _drop_columns_ch( { ch => $ch, table_name => $names_tbl, col => \@col_to_drop, %$param_href } );
# Purpose    : to drop columns from table
# Returns    : nothing
# Parameters : databse handle, table name, columns to drop (aref), and rest of params
# Throws     : croaks if wrong number of parameters
# Comments   : fast because it drops files (columnar format)
# See Also   :
sub _drop_columns_ch {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_drop_columns_ch() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # drop unnecessary columns
    my @col_to_drop    = @{ $param_href->{col} };
    my $column_list    = join ", ", @col_to_drop;
    my $droplist       = 'DROP COLUMN ' . join ", DROP COLUMN ", @col_to_drop;
    my $drop_col_query = qq{ ALTER TABLE $param_href->{database}.$param_href->{table_name} $droplist };

    eval { $param_href->{ch}->do($drop_col_query) };
    $log->error(
        "Error: dropping columns {$column_list} from {$param_href->{database}.$param_href->{table_name}} failed: $@")
      if $@;
    $log->info(
        "Action: columns {$column_list} from {$param_href->{database}.$param_href->{table_name}} dropped successfully")
      unless $@;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=queue_and_run
# Purpose    : to run all steps for multiple blast output files
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : runs sequentially for each blast output file
# See Also   : 
sub queue_and_run {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('queue_and_run() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $in       = $param_href->{in}       or $log->logcroak('no --in=dir specified on command line!');
    my $database = $param_href->{database} or $log->logcroak('no --database=db_name specified on command line!');
    my $names    = $param_href->{names}    or $log->logcroak('no --names=names_tsv specified on command line!');

    # create database if not exists (it doesn't drop it)
    _create_db_only($param_href);

    # create support tbl to hold information about organisms processed
    my $import_q = _create_support_tbl($param_href);

    # import names file
    my $names_tbl = import_names($param_href);

    # get all stats, maps and blastout files into a hash
    my $organism_href = _collect_input_files($in);

    # now run import and analysis for each organism
  ORGANISM:
    foreach my $org_name ( sort keys %{$organism_href} ) {

        #while ( my ( $org_name, $files_href ) = each %{$organism_href} ) {
        $log->warn("Working with $org_name!");
        my $files_href = $organism_href->{$org_name};

        # check if organism already exists (if yes skip)
        my $ch        = _get_ch($param_href);
        my $organisms_aref_of_arefs = $ch->select("SELECT organism FROM $param_href->{database}.support ORDER BY organism");
        my @organisms = map { $_->[0] } @{$organisms_aref_of_arefs};
        my $org_in_db_print = sprintf( Data::Dumper->Dump( [ \@organisms ], [ qw(*organisms_in_database) ]) );
        $log->debug("$org_in_db_print");
        foreach my $organism (@organisms) {
            if ( $organism eq $org_name ) {
                $log->warn("Skip: $organism already in database");
                next ORGANISM;
            }
        }

        # import files for each organism
        my $map_tbl = import_map( { %$param_href, map => $files_href->{map} } );
        my ( $stats_ps_tbl, $stats_gen_tbl ) = import_blastdb_stats( { %$param_href, stats => $files_href->{stats} } );
        my $blastout_tbl = import_blastout( { %$param_href, blastout => $files_href->{blastout}, } );

        # run analysis on blastout_tbl to produce report_per_species
        my ( $blastout_uniq_tbl, $blout_species_tbl, $report_ps_tbl ) = blastout_uniq(
            {   %$param_href,
                blastout_tbl  => $blastout_tbl,
                stats_gen_tbl => $stats_gen_tbl,
                map_tbl       => $map_tbl,
                names_tbl     => $names_tbl,
            }
        );
        my $report_exp_tbl = bl_uniq_exp_iter( { %$param_href, report_ps_tbl => $report_ps_tbl } );

        # import table names into support table to know if organism processed
        my $ch2      = _get_ch($param_href);
        my $arg_list = [
            "$org_name",      "$files_href->{ti}", "$map_tbl",           "$names_tbl",
            "$stats_gen_tbl", "$stats_ps_tbl",     "$blastout_uniq_tbl", "$blout_species_tbl",
            "$report_ps_tbl", "$report_exp_tbl"
        ];
        eval { $ch2->do( $import_q, $arg_list ) };
        $log->error("Error: inserting into {$param_href->{database}.support} failed: $@") if $@;
        $log->info("Action: {$param_href->{database}.support} inserted successfully for {$org_name}") unless $@;
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : my $organism_href = _collect_input_files($in);
# Purpose    : collects files (stats, maps, blast outpusts) for queue_and_run()
# Returns    : hash ref of list of files
# Parameters : input directorey where to search for files
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : queue_and_run()
sub _collect_input_files {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_collect_input_files() needs a $param_href') unless @_ == 1;
    my ($in) = @_ or $log->logcroak('no $in_dir sent to _collect_input_files()!');

    # get all stats files
    my @stats_files = File::Find::Rule->file()->name(qr/\A.+\.analyze\z/)->in($in);
    @stats_files = sort { $a cmp $b } @stats_files;
    my $stats_print = sprintf( Data::Dumper->Dump( [ \@stats_files ], [qw(*stats_files)] ) );
    $log->debug("$stats_print");

    # get all phylo_maps for organisms
    my @map_files = File::Find::Rule->file()->name(qr/\A.+\.phmap_names\z/)->in($in);
    @map_files = sort { $a cmp $b } @map_files;
    my $map_print = sprintf( Data::Dumper->Dump( [ \@map_files ], [qw(*map_files)] ) );
    $log->debug("$map_print");

    # get all blastout files from indir
    my @blastout_files = File::Find::Rule->file()->name(qr/\A.+\.gz\z/)->in($in);
    @blastout_files = sort { $a cmp $b } @blastout_files;
    my $blout_print = sprintf( Data::Dumper->Dump( [ \@blastout_files ], [qw(*blastout_files)] ) );
    $log->debug("$blout_print");

    # check for all files (each blastout needs map and stats)
    my %organisms;
  BLASTOUT:
    foreach my $blastout (@blastout_files) {
        my $bl_name = path($blastout)->basename;
        #say "BL_NAME:$bl_name";
        ( my $organism ) = $bl_name =~ m/\A(.+?)\_.+\z/;
        #say "ORG:$organism";

        foreach my $map (@map_files) {
            my $map_name = path($map)->basename;
            #say "MAP_NAME:$map_name";
            ( my $organism_map ) = $map_name =~ m/\A([^\d]+)(?:\d+|\_)*.+\z/;
            #say "ORG_MAP:$organism_map";

            foreach my $stat (@stats_files) {
                my $stat_name = path($stat)->basename;
                #say "STAT_NAME:$stat_name";
                my ( $organism_stat, $org_ti ) = $stat_name =~ m/\A(.+?)\_(\d+).+\z/;
                #say "ORG_stat:$organism_stat";

                if ( $organism_map eq $organism && $organism_stat eq $organism ) {

                    # put files under organism key
                    $organisms{$organism} = { blastout => $blastout, map => $map, stats => $stat, ti => $org_ti };
                    next BLASTOUT;    # if found go to nest blast output
                }
            }
        }
    }

    my $org_print = sprintf( "%s", Data::Dumper->Dump( [ \%organisms ], [qw(*organisms)] ) );
    $log->debug("$org_print");

    return \%organisms;
}


### INTERFACE SUB ###
# Usage      : --mode=exclude_ti_from_blastout();
# Purpose    : excludes tax_id from blastout file and saves new file to disk
# Returns    : nothing
# Parameters : ($param_href)
# Throws     : croaks for parameters
# Comments   : works with gziped file
# See Also   : 
sub exclude_ti_from_blastout {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('exclude_ti_from_blastout() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{blastout} or $log->logcroak('no $infile specified on command line!');
    my $tax_id = $param_href->{tax_id}   or $log->logcroak('no $tax_id specified on command line!');
    my $blastout = path($infile)->basename;
    ($blastout = $blastout) =~ s/\A(.+)\.gz\z/$1/;
    my $blastout_good = path( path($infile)->parent, $blastout . "_good.gz" );
    my $blastout_bad  = path( path($infile)->parent, $blastout . "_bad.gz" );

    open( my $blastout_fh, "pigz -c -d $infile |" ) or $log->logdie("Error: blastout file $infile not found:$!");
    open( my $blastout_good_fh, ">:gzip", $blastout_good )
      or $log->logdie("Error: can't open good output {$blastout_good} for writing:$!");
    open( my $blastout_bad_fh, ">:gzip", $blastout_bad )
      or $log->logdie("Error: can't open bad output {$blastout_bad} for writing :$!");

#in blastout
#ENSG00000151914|ENSP00000354508    pgi|34252924|ti|9606|pi|0|  100.00  7461    0   0   1   7461    1   7461    0.0 1.437e+04

    local $.;
    my $i_good = 0;
    my $i_bad  = 0;
    while (<$blastout_fh>) {
        chomp;
        my ( undef, $id, undef, undef, undef, undef, undef, undef, undef, undef, undef, undef ) = split "\t", $_;
        my ( undef, undef, undef, $ti, undef, undef ) = split( /\|/, $id );    #pgi|0000000000042857453|ti|428574|pi|0|
                # any string that is not a single space (chr(32)) will implicitly be used as a regex, so split '|' will still be split /|/ and thus equal split //

        #progress tracker
        if ( $. % 1000000 == 0 ) {
            $log->trace("$. lines processed!");
        }

        #if found bad id exclude from blastout
        if ( $ti == $tax_id ) {
            $i_bad++;
            say {$blastout_bad_fh} $_;
        }
        else {
            $i_good++;
            say {$blastout_good_fh} $_;
        }

    }

    #give info about what you did
    $log->info("Report: file $blastout read successfully with $. lines");
    $log->info("Report: file $blastout_good printed successfully with $i_good lines");
    $log->info("Report: file $blastout_bad printed successfully with $i_bad lines");

    close $blastout_fh;
    close $blastout_good_fh;
    close $blastout_bad_fh;

    return;
}


### INTERFACE SUB ###
# Usage      : _create_db_only($param_href);
# Purpose    : creates database in ClickHouse (it doesn't drop it)
# Returns    : nothing
# Parameters : ( $param_href ) -> params from command line to connect to ClickHouse
#            : -d (database name)
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   :
sub _create_db_only {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_create_db_only() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $database     = $param_href->{database} or $log->logcroak('no $database specified on command line!');
    my $query_create = qq{CREATE DATABASE IF NOT EXISTS $database};

    # create database
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Action: creating $database failed!") unless $success_create;
    $log->info("Action: database {$database} created successfully!") if $success_create;

    return;
}


### INTERNAL UTILITY ###
# Usage      : _create_support_tbl( $param_href );
# Purpose    : creates support table to hold information about organisms processed
# Returns    : nothing
# Parameters :
# Throws     : croaks if wrong number of parameters
# Comments   :
# See Also   :
sub _create_support_tbl {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_create_support_tbl() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # get new handle
    my $ch = _get_ch($param_href);

    # create support_tbl
    my $create_q = qq{
    CREATE TABLE IF NOT EXISTS support (
    species_name String,
    organism String,
    ti UInt32,
    map_tbl String,
    names_tbl String,
    stats_gen_tbl String,
    stats_ps_tbl String,
    blastout_uniq_tbl String,
    blastout_uniq_species_tbl String,
    report_ps_tbl String,
    report_exp_tbl String,
    date Date  DEFAULT today()
    )ENGINE=MergeTree (date, (organism), 8192)
    };
    _create_table_ch( { table_name => 'support', ch => $ch, query => $create_q, drop => 0, %{$param_href} } );

    # build query to import back to database (needed later)
    my $import_q
      = qq{INSERT INTO $param_href->{database}.support (organism, ti, map_tbl, names_tbl, stats_gen_tbl, stats_ps_tbl, blastout_uniq_tbl, blastout_uniq_species_tbl, report_ps_tbl, report_exp_tbl) VALUES};
    $log->trace("$import_q");

    # example
    # INSERT INTO kam.support (organism, ti, map_tbl, names_tbl, stats_gen_tbl, stats_ps_tbl, blastout_uniq_tbl, blastout_uniq_species_tbl, report_ps_tbl, report_exp_tbl) VALUES ('an', '111111', 'an3_map', 'names_dmp_fmt_new', 'an_28377_analyze_stats_genomes', 'an_28377_analyze_stats_ps', 'an_fulldb_plus_22_03_2016_good_uniq', 'an_fulldb_plus_22_03_2016_good_uniq_species', 'an_fulldb_plus_22_03_2016_good_report_per_species', 'an_fulldb_plus_22_03_2016_good_report_per_species_expanded');

    return $import_q;
}


### INTERFACE SUB ###
# Usage      : --mode=dump_chdb
# Purpose    : export a single table from database or entire database
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : it writes pigz compressed files
# See Also   : 
sub dump_chdb {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('dump_chdb() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $database = $param_href->{database} or $log->logcroak('no $database specified on command line!');
    my $table_ch = $param_href->{table_ch};
    my $out      = $param_href->{out} or $log->logcroak('no $out specified on command line!');
    my $format_ex = defined $param_href->{format_ex} ? $param_href->{format_ex} : 'Native';    # or TabSeparated

    # dump only table if specified
    if ($table_ch) {
        _dump_table_only( $param_href );
    }
	# else dump all tables in a database
    else {
        _dump_entire_db( $param_href );
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : _dump_table_only( $param_href);
# Purpose    : export a single table from ClickHouse
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub _dump_table_only {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_dump_table_only() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # dump metadata
    my $ch_tbl_to_export = "$param_href->{database}.$param_href->{table_ch}";
    my $tbl_create_sql = path( $param_href->{out}, "$param_href->{database}." . $param_href->{table_ch} . '.sql' );
    my $cmd
      = qq{clickhouse-client --query="SHOW CREATE TABLE $ch_tbl_to_export" --format=TabSeparatedRaw > $tbl_create_sql};
    my ( $stdout, $stderr, $exit ) = _capture_output( $cmd, $param_href );
    if ( $exit == 0 ) {
        $log->debug("Action: table {$ch_tbl_to_export} metadata exported to $tbl_create_sql");
    }
    else {
        $log->logdie("Error: $cmd failed: $stderr");
    }

    # dump table contents
    my $tbl_dump = path( $param_href->{out}, "$param_href->{database}." . $param_href->{table_ch} . '.' . $param_href->{format_ex} . '.gz' );
    my $cmd_tbl
      = qq{clickhouse-client --query="SELECT * FROM $ch_tbl_to_export FORMAT $param_href->{format_ex}" | pigz > $tbl_dump};
    my ( $stdout2, $stderr2, $exit2 ) = _capture_output( $cmd_tbl, $param_href );
    if ( $exit2 == 0 ) {
        $log->info("Action: table {$ch_tbl_to_export} exported to $tbl_dump");
    }
    else {
        $log->logdie("Error: $cmd_tbl failed: $stderr2");
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : _dump_entire_db( $param_href );
# Purpose    : to dump all tables in a database
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : _dump_table_only()
sub _dump_entire_db {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_dump_entire_db() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # if not specified don't run in parallel
    my $max_processes = defined $param_href->{max_processes} ? $param_href->{max_processes} : 1;

    # connect to database
    my $ch = _get_ch($param_href);

    # select all tables in a database
    my $select_tbl_q = qq{ SELECT name FROM system.tables WHERE database = '$param_href->{database}' };
    my $tbl_aref     = $ch->select($select_tbl_q);
    my @tables       = map { $_->[0] } @{$tbl_aref};

    # dump them in parallel
    my $pm = Parallel::ForkManager->new($max_processes);
  DUMP:
    foreach my $tbl (@tables) {
        my $pid = $pm->start and next DUMP;
        _dump_table_only( { %{$param_href}, table_ch => $tbl } );
        $pm->finish;
    }
    $pm->wait_all_children;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=restore_chdb
# Purpose    : imports tables into database
# Returns    : nothing
# Parameters : $param_href->{tbl_sql} and $param_href->{tbl_contents} for table import
#            : $param_href->{in} for database import
#            : $param_href->{database} for both
# Throws     : croaks if wrong number of parameters
# Comments   : works in parallel for entire database
#            : 2 modes: table and entire database import
# See Also   : 
sub restore_chdb {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('restore_chdb() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $database = $param_href->{database} or $log->logcroak('no $database specified on command line!');
    my $tbl_sql  = $param_href->{tbl_sql};
    my $tbl_contents = $param_href->{tbl_contents};

    # restore only one table if specified
    if ($tbl_sql and $tbl_contents) {
        _restore_table($param_href);
    }

    # else restore all tables in a database
    else {
		_restore_entire_db($param_href);
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : _restore_table($param_href);
# Purpose    : to restore a single table into database
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : it can drop table
#            : it can change database where it imports
# See Also   : 
sub _restore_table {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_restore_table() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # handle filenames
    my $tbl_sql        = $param_href->{tbl_sql};
    my $tbl_sql_abs    = path( $param_href->{tbl_sql} )->absolute;
    my $tbl_to_restore = path( $param_href->{tbl_sql} )->basename;
    $tbl_to_restore =~ s/\.sql//g;
    my ( $db_name, $tbl_name ) = $tbl_to_restore =~ m/\A([^\.]+)\.(.+)\z/;

    # drop table if specified with --drop_tbl
    my $drop_tbl = defined $param_href->{drop_tbl} ? $param_href->{drop_tbl} : 0;    # 0 means do not drop
    if ($drop_tbl) {
        my $drop_tbl_q = qq{DROP TABLE $param_href->{database}.$tbl_name};
        my $cmd_drop   = qq{clickhouse-client --query="$drop_tbl_q"};
        my ( $stdout_drop, $stderr_drop, $exit_drop ) = _capture_output( $cmd_drop, $param_href );
        if ( $exit_drop == 0 ) {
            $log->debug("Action: table {$param_href->{database}.$tbl_name} deleted");
        }
        else {
            $log->debug("Report: table {$tbl_name} doesn't exist in {$param_href->{database}}: $stderr_drop");
        }
    }

    # change SQL if not equal
    if ( $db_name ne $param_href->{database} ) {

        # Get a handle on the code...
        open my $sql_fh, '<', $tbl_sql_abs or $log->logdie("Error: can't open sqlfile {$tbl_sql_abs}: $!");

        # Read it all in...
        my $create_sql = do { local $/; <$sql_fh> };
        $create_sql =~ s/CREATE TABLE ([^\.]+)\./CREATE TABLE $param_href->{database}\./;

        # restore metadata
        my $cmd_meta_changed = qq{clickhouse-client --query="$create_sql"};
        my ( $stdout_meta_changed, $stderr_meta_changed, $exit_meta_changed )
          = _capture_output( $cmd_meta_changed, $param_href );
        if ( $exit_meta_changed == 0 ) {
            $log->debug("Action: table {$param_href->{database}.$tbl_name} created");
        }
        else {
            $log->logdie("Error: $cmd_meta_changed failed: $stderr_meta_changed");
        }

    }
    else {
        # restore metadata
        my $cmd_meta = qq{clickhouse-client < $tbl_sql_abs};
        my ( $stdout_meta, $stderr_meta, $exit_meta ) = _capture_output( $cmd_meta, $param_href );
        if ( $exit_meta == 0 ) {
            $log->debug("Action: table {$tbl_to_restore} created");
        }
        else {
            $log->logdie("Error: $cmd_meta failed: $stderr_meta");
        }
    }

    # PART 2: restore table contents
    my $tbl_contents     = $param_href->{tbl_contents};
    my $tbl_contents_abs = path( $param_href->{tbl_contents} )->absolute;
    my $file_to_restore  = path( $param_href->{tbl_contents} )->basename;
    $file_to_restore =~ s/\.gz//;
    my ( $db_name_from_file, $tbl_name_from_file, $format ) = $file_to_restore =~ m/\A([^\.]+)\.([^\.]+)\.(.+)\z/;
    my $cmd_content
      = qq{pigz -c -d $tbl_contents_abs | clickhouse-client --query="INSERT INTO $param_href->{database}.$tbl_name_from_file FORMAT $format"};
    my ( $stdout_content, $stderr_content, $exit_content ) = _capture_output( $cmd_content, $param_href );

    if ( $exit_content == 0 ) {
        $log->info("Action: table {$param_href->{database}.$tbl_name_from_file} restored from $tbl_contents_abs");
    }
    else {
        $log->logdie("Error: $cmd_content failed: $stderr_content");
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : _restore_entire_db( $param_href );
# Purpose    : to restore all tables in a directory to a database
# Returns    : nothing
# Parameters : needs --in
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : _restore_table()
sub _restore_entire_db {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_restore_entire_db() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # if not specified don't run in parallel
    my $max_processes = defined $param_href->{max_processes} ? $param_href->{max_processes} : 1;

    # collect all tables in a --in directory
    my $in = $param_href->{in} or $log->logcroak('no $in specified on command line!');
    my @sql_def = File::Find::Rule->file()->name('*.sql')->in($in);
    @sql_def = sort { $a cmp $b } @sql_def;
    #print Dumper( \@sql_def );
    my @sql_content = File::Find::Rule->file()->name('*.gz')->in($in);
    @sql_content = sort { $a cmp $b } @sql_content;
    #print Dumper( \@sql_content );

    # create hash pairs (sql => tbl_content) to send to _restore_table()
    my %sql_plus_content_pair;
    if ( scalar @sql_def == scalar @sql_content ) {

        # check sql files
      SQL:
        foreach my $sql (@sql_def) {
            my $sql_name = path($sql)->basename;
            $sql_name =~ s/\.sql//;
            #say "SQL_NAME:$sql_name";

            # check sql contents
            foreach my $content (@sql_content) {
                my $content_name = path($content)->basename;
                $content_name =~ s{\A([^\.]+)\.([^\.]+)\..+\z}{$1\.$2};
                #say "CONTENT_NAME:$content_name";

                # if match found join them in hash
                if ( $sql_name eq $content_name ) {
                    $sql_plus_content_pair{$sql} = $content;
                    next SQL;
                }
            }
        }
    }
    else {
        $log->logdie("Error: missing files in $in");
    }
    #print Dumper( \%sql_plus_content_pair );

    # count number of tables to be restored
    my $restore_cnt = keys %sql_plus_content_pair;
    $log->info("Report: restoring {$restore_cnt} tables from $in to database:$param_href->{database}");

    # restore them in parallel
    my $pm = Parallel::ForkManager->new($max_processes);
  RESTORE:
    foreach my $tbl_sql ( keys %sql_plus_content_pair ) {
        my $pid = $pm->start and next RESTORE;
        _restore_table( { %{$param_href}, tbl_sql => $tbl_sql, tbl_contents => $sql_plus_content_pair{$tbl_sql} } );
        $pm->finish;
    }
    $pm->wait_all_children;

    return;
}



### INTERFACE SUB ###
# Usage      : --mode=top_hits
# Purpose    : selects top hits from blastout_uniq-report_per_ps_expanded tables
# Returns    : name of the resulting table
# Parameters : $param_href
# Throws     : croaks if wrong number of parameters
# Comments   :
# See Also   :
sub top_hits {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('top_hits() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    # take top 10 if not defined on command line
    my $top_hits = defined $param_href->{top_hits} ? $param_href->{top_hits} : 10;
    my $top_hits_tbl = 'top_hits' . "$param_href->{top_hits}";

    # connect to database
    my $ch = _get_ch($param_href);

    # create top_hits table
    my $create_top_hits_q = qq{
    CREATE TABLE $top_hits_tbl (
    ti UInt32,
    species_name String,
    gene_hits_per_species UInt64,
    hits1 UInt32,
    hits2 UInt32,
    hits3 UInt32,
    hits4 UInt32,
    hits5 UInt32,
    hits6 UInt32,
    hits7 UInt32,
    hits8 UInt32,
    hits9 UInt32,
    hits10 UInt32,
    date Date  DEFAULT today() )
    ENGINE=MergeTree (date, ti, 8192) };

    _create_table_ch( { table_name => $top_hits_tbl, ch => $ch, query => $create_top_hits_q, %$param_href } );
    $log->trace("Report: $create_top_hits_q");

    # select all expanded tables in a support table
    my $select_exp_q = qq{ SELECT report_exp_tbl FROM $param_href->{database}.support };
    my $tbl_aref     = $ch->select($select_exp_q);
    my @exp_tables   = map { $_->[0] } @{$tbl_aref};
    print Dumper( \@exp_tables );

    # select top 10 hits from table
    foreach my $exp_tbl (@exp_tables) {
        my $ins_hits_q = qq{
        INSERT INTO $top_hits_tbl (ti, species_name, gene_hits_per_species, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10)
        SELECT ti, species_name, gene_hits_per_species, hits1, hits2, hits3, hits4, hits5, hits6, hits7, hits8, hits9, hits10
        FROM $param_href->{database}.$exp_tbl
        WHERE ps = 1
        ORDER BY gene_hits_per_species DESC
        LIMIT $top_hits
        };

        $log->trace($ins_hits_q);
        eval { $ch->do($ins_hits_q); };
        $log->error("Error: inserting {$param_href->{database}.$top_hits_tbl} failed for $exp_tbl: $@") if $@;
        $log->info("Action: {$param_href->{database}.$top_hits_tbl} inserted from $exp_tbl") unless $@;
    }

    return $top_hits_tbl;
}


1;
__END__

=encoding utf-8

=head1 NAME

FindOrigin - It's a modulino used to analyze BLAST output and database in ClickHouse (columnar DBMS for OLAP).

=head1 SYNOPSIS

    # drop and recreate database (connection parameters in blastoutanalyze.cnf)
    FindOrigin.pm --mode=create_db -d test_db_here

    # import BLAST output file into ClickHouse database
    FindOrigin.pm --mode=import_blastout -d jura --blastout=t/data/hs_all_plus_21_12_2015.gz

    # remove header and import phylostratigraphic map into ClickHouse database (reads PS, TI and PSNAME from config)
    FindOrigin.pm --mode=import_map -d jura --map t/data/hs3.phmap_names -v

    # imports analyze stats file created by AnalyzePhyloDb (uses TI and PS sections in config)
    FindOrigin.pm --mode=import_blastdb_stats -d jura --stats=t/data/analyze_hs_9606_all_ff_for_db -v

    # import names file for species_name
    FindOrigin.pm --mode=import_names -d jura --names=t/data/names.dmp.fmt.new.gz -v

    # runs BLAST output analysis - expanding every prot_id to its tax_id hits and species names
    FindOrigin.pm --mode=blastout_uniq -d hs_plus -v

    # update report_ps_tbl table with unique and intersect hts and gene lists
    FindOrigin.pm --mode=bl_uniq_expanded -d jura --report_ps_tbl=hs_1mil_report_per_species -v -v

    # import full BLAST database (plus ti and pgi columns)
    FindOrigin.pm --mode=import_blastdb --blastdb t/data/db90_head.gz -d dbfull -v -v

    # run import and analysis for all blast output files
    FindOrigin.pm --mode=queue_and_run -d kam --in=/msestak/blastout/ --names t/data/names.dmp.fmt.new.gz -v -v

    # removes specific hits from the BLAST output based on the specified tax_id (exclude bad genomes).
    FindOrigin.pm --mode=exclude_ti_from_blastout --blastout t/data/hs_all_plus_21_12_2015.gz -ti 428574 -v

    # dump a single table
    FindOrigin.pm --mode=dump_chdb --database=kam --out=/msestak/blastout/ --format_ex=Native --table_ch=names_dmp_fmt_new

    # dump all tables in a database
    FindOrigin.pm --mode=dump_chdb --database=kam --out=/msestak/blastout/ --format_ex=Native --max_processes=8

    # restore a single table
    FindOrigin.pm --mode=restore_chdb --database=jura --tbl_sql=/msestak/blastout/kam.am3_map.sql --tbl_contents=/msestak/blastout/kam.am3_map.Native.gz
    FindOrigin.pm --mode=restore_chdb --database=jura --tbl_sql=/msestak/blastout/kam.am3_map.sql --tbl_contents=/msestak/blastout/kam.am3_map.Native.gz --drop_tbl

    # restore all tables to a database
    FindOrigin.pm --mode=restore_chdb --database=kam --in=/msestak/blastout/ --max_processes=8
    FindOrigin.pm --mode=restore_chdb --database=kam --in=/msestak/blastout/ --max_processes=8 --drop_tbl

    # find top hits N for all species in a database
    FindOrigin.pm --mode=top_hits -d kam --top_hits=10



=head1 DESCRIPTION

FindOrigin is modulino used to analyze BLAST database (to get content in genomes and sequences) and BLAST output (to figure out where are hits coming from). It includes config, command-line and logging management.

 For help write:
 FindOrigin.pm -h
 FindOrigin.pm -m

=head2 MODES

=over 4

=item create_db

 # options from command line
 FindOrigin.pm --mode=create_db -ho localhost -d test_db_here -po 8123

 # options from config
 FindOrigin.pm --mode=create_db -d test_db_here

Drops ( if it exists) and recreates database in ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).

=item import_blastout

 # options from command line
 FindOrigin.pm --mode=import_blastout -d jura --blastout t/data/hs_all_plus_21_12_2015.gz -ho localhost -po 8123 -v

 # options from config
 FindOrigin.pm --mode=import_blastout -d jura --blastout t/data/hs_all_plus_21_12_2015.gz

Imports compressed BLAST output file (.gz needs pigz) into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
It drops and recreates table in a database where it will import it and runs separate query at end to return number of rows inserted.

=item import_map

 # options from command line
 FindOrigin.pm --mode=import_map -d jura --map t/data/hs3.phmap_names -ho localhost -po 8123 -v

 # options from config
 FindOrigin.pm --mode=import_map -d jura --map t/data/hs3.phmap_names -v

Removes header from map file and writes columns (prot_id, phylostrata, ti, psname) to tmp file and imports that file into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
It can use PS, TI and PSNAME config sections.

=item import_blastdb_stats

 # options from command line
 FindOrigin.pm --mode=import_blastdb_stats -d jura --stats=t/data/analyze_hs_9606_all_ff_for_db -ho localhost -po 8123 -v

 # options from config
 FindOrigin.pm --mode=import_blastdb_stats -d jura --stats=t/data/analyze_hs_9606_all_ff_for_db -v

Imports analyze stats file created by AnalyzePhyloDb.
 AnalysePhyloDb -n nr_raw/nodes.dmp.fmt.new.sync -t 9606 -d ./all_ff_for_db/ > analyze_hs_9606_all_ff_for_db

It can use PS and TI config sections.

=item import_names

 # options from command line
 FindOrigin.pm --mode=import_names -d jura --names=t/data/names.dmp.fmt.new.gz -ho localhost -po 8123 -v

 # options from config
 FindOrigin.pm --mode=import_names -d jura --names=t/data/names.dmp.fmt.new.gz -v

Imports names file (columns ti, species_name) into ClickHouse.

=item blastout_uniq

 # options from command line
 FindOrigin.pm --mode=blastout_uniq -d jura --blastout_tbl=hs_1mil -v

 # options from config
 FindOrigin.pm --mode=blastout_uniq -d jura --blastout_tbl=hs_1mil -v

It creates a unique non-redundant blastout_uniq table with only relevant information (prot_id, ti) for stratification and other purposes. Other columns (score, pgi blast hit) could be added later too.
From that blastout_uniq_tbl it creates report_gene_hit_per_species_tbl2 which holds summary of per phylostrata per species of BLAST output analysis (ps, ti, species_name, gene_hits_per_species, genelist).

=item bl_uniq_expanded

 # grabs all genelists at once, large memory consumption, but faster
 FindOrigin.pm --mode=bl_uniq_expanded -d jura --report_ps_tbl=hs_1mil_report_per_species -v -v

 # works iteratively, less memory, but slower
 FindOrigin.pm --mode=bl_uniq_exp_iter -d jura --report_ps_tbl=hs_1mil_report_per_species -v -v

Update report_ps_tbl table with unique and intersect hits and gene lists.

=item exclude_ti_from_blastout

 # options from command line
 lib/BlastoutAnalyze.pm --mode=exclude_ti_from_blastout --blastout t/data/hs_all_plus_21_12_2015.gz -ti 428574 -v

 # options from config
 lib/BlastoutAnalyze.pm --mode=exclude_ti_from_blastout --blastout t/data/hs_all_plus_21_12_2015.gz -ti 428574 -v

Removes specific hits from the BLAST output based on the specified tax_id (exclude bad genomes). It works with gziped BLAST output and writes gziped files.


=item import_blastdb

 # options from config
 FindOrigin.pm --mode=import_blastdb --blastdb t/data/db90_head.gz -d dbfull -v -v
 # runs in 2 h for 113,834,350 fasta records
 FindOrigin.pm --mode=import_blastdb -d blastdb --blastdb /msestak/dbfull/dbfull.gz -v -v

Imports BLAST database file into ClickHouse (it splits prot_id into 2 extra columns = ti and pgi). It needs ClickHouse connection parameters to connect to ClickHouse.

=item queue_and_run

 # options from command line
 FindOrigin.pm --mode=queue_and_run -d kam --in=/msestak/blastout/ --names t/data/names.dmp.fmt.new.gz -v -v

Imports all BLAST output files in a given directory and calculates unique hits per species for all one by one.
It first (re)creates database where it will run, imports names file only once and collects BLAST output, stats and map files and imports all these triplets.

=item dump_chdb

 # dump a single table
 FindOrigin.pm --mode=dump_chdb --database=kam --out=/msestak/blastout/ --format_ex=Native --table_ch=names_dmp_fmt_new

 # dump all tables in a database
 FindOrigin.pm --mode=dump_chdb --database=kam --out=/msestak/blastout/ --format_ex=Native --max_processes=8

Exports a single table or all tables from a database. It exports both metadata (create table) and table contents.
Native is the most efficient format. CSV, TabSeparated, JSONEachRow are more portable: you may import/export data to another DBMS.
It can run in parallel if --max_processes specified.

=item restore_chdb

 # restore a single table
 FindOrigin.pm --mode=restore_chdb --database=jura --tbl_sql=/msestak/blastout/kam.am3_map.sql --tbl_contents=/msestak/blastout/kam.am3_map.Native.gz
 FindOrigin.pm --mode=restore_chdb --database=jura --tbl_sql=/msestak/blastout/kam.am3_map.sql --tbl_contents=/msestak/blastout/kam.am3_map.Native.gz --drop_tbl

 # restore all tables to a database
 FindOrigin.pm --mode=restore_chdb --database=kam --in=/msestak/blastout/ --max_processes=8
 FindOrigin.pm --mode=restore_chdb --database=kam --in=/msestak/blastout/ --max_processes=8 --drop_tbl

Restores a single table or all tables from a directory. It creates (optionally drops) a table and imports table contents. It uses pigz to decompress table contents.
It can run in parallel if --max_processes specified.

=item top_hits

 # find top hits N for all species in a database
 FindOrigin.pm --mode=top_hits -d kam --top_hits=10

It finds top N species with most BLAST hits (proteins found) in prokaryotes.


=back

=head1 CONFIGURATION

All configuration in set in blastoutanalyze.cnf that is found in ./lib directory (it can also be set with --config option on command line). It follows L<< Config::Std|https://metacpan.org/pod/Config::Std >> format and rules.
Example:

 [General]
 #in       = t/data/
 out      =  t/data/
 #infile   = t/data/
 #outfile  = t/data/
 
 [Database]
 host      = localhost
 database  = test
 #user     = default
 #password = ''
 port      = 8123
 
 [Tables]
 blastout_tbl   = hs_1mil
 names_tbl      = names_dmp_fmt_new
 map_tbl        = hs3_map
 stats_ps_tbl   = analyze_hs_9606_all_ff_for_db_stats_ps
 stats_gen_tbl  = analyze_hs_9606_all_ff_for_db_stats_genomes
 blastdb_tbl    = ''
 report_ps_tbl  = hs_1mil_report_per_species
 report_exp_tbl = hs_1mil_report_per_species_expanded
 
 [Files]
 blastdb  = t/data/dbfull.gz
 blastout = t/data/hs_all_plus_21_12_2015.gz
 map      = t/data/hs3.phmap_names
 names    = t/data/names.dmp.fmt.new.gz
 stats    = t/data/analyze_hs_9606_all_ff_for_db
 
 [PS]
 1   =  1
 2   =  2
 3   =  2
 4   =  2
 5   =  3
 6   =  4
 7   =  4
 8   =  4
 9   =  5
 10  =  6
 11  =  6
 12  =  7
 13  =  8
 14  =  9
 15  =  10
 16  =  11
 17  =  11
 18  =  11
 19  =  12
 20  =  12
 21  =  12
 22  =  13
 23  =  14
 24  =  15
 25  =  15
 26  =  16
 27  =  17
 28  =  18
 29  =  18
 30  =  19
 31  =  19
 32  =  19
 33  =  19
 34  =  19
 35  =  19
 36  =  19
 37  =  19
 38  =  19
 39  =  19
 
 [TI]
 131567   =  131567
 2759     =  2759
 1708629  =  2759
 1708631  =  2759
 33154    =  33154
 1708671  =  1708671
 1708672  =  1708671
 1708673  =  1708671
 33208    =  33208
 6072     =  6072
 1708696  =  6072
 33213    =  33213
 33511    =  33511
 7711     =  7711
 1708690  =  1708690
 7742     =  7742
 7776     =  7742
 117570   =  7742
 117571   =  117571
 8287     =  117571
 1338369  =  117571
 32523    =  32523
 32524    =  32524
 40674    =  40674
 32525    =  40674
 9347     =  9347
 1437010  =  1437010
 314146   =  314146
 1708730  =  314146
 9443     =  9443
 376913   =  9443
 314293   =  9443
 9526     =  9443
 314295   =  9443
 9604     =  9443
 207598   =  9443
 1708693  =  9443
 9605     =  9443
 9606     =  9443
 
 [PSNAME]
 cellular_organisms  =  cellular_organisms
 Eukaryota  =  Eukaryota
 Unikonta  =  Eukaryota
 Apusozoa/Opisthokonta  =  Eukaryota
 Opisthokonta  =  Opisthokonta
 Holozoa  =  Holozoa
 Filozoa  =  Holozoa
 Metazoa/Choanoflagellida  =  Holozoa
 Metazoa  =  Metazoa
 Eumetazoa  =  Eumetazoa
 Cnidaria/Bilateria  =  Eumetazoa
 Bilateria  =  Bilateria
 Deuterostomia  =  Deuterostomia
 Chordata  =  Chordata
 Olfactores  =  Olfactores
 Vertebrata  =  Vertebrata
 Gnathostomata  =  Vertebrata
 Teleostomi  =  Vertebrata
 Euteleostomi  =  Euteleostomi
 Sarcopterygii  =  Euteleostomi
 Dipnotetrapodomorpha  =  Euteleostomi
 Tetrapoda  =  Tetrapoda
 Amniota  =  Amniota
 Mammalia  =  Mammalia
 Theria  =  Mammalia
 Eutheria  =  Eutheria
 Boreoeutheria  =  Boreoeutheria
 Euarchontoglires  =  Euarchontoglires
 Scandentia/Primates  =  Euarchontoglires
 Primates  =  Primates
 Haplorrhini  =  Primates
 Simiiformes  =  Primates
 Catarrhini  =  Primates
 Hominoidea  =  Primates
 Hominidae  =  Primates
 Homininae  =  Primates
 Hominini  =  Primates
 Homo  =  Primates
 Homo_sapiens  =  Primates

=head1 LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Martin Sebastijan Šestak
mocnii
E<lt>msestak@irb.hrE<gt>

=cut
