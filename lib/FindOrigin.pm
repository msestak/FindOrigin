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
use DBI;
use DBD::mysql;
use DateTime::Tiny;
use POSIX qw(mkfifo);
use HTTP::Tiny;

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
    my $verbose  = $param_href->{verbose};
    my $quiet    = $param_href->{quiet};
    my @mode     = @{ $param_href->{mode} };

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
    my $param_print = sprintf( Dumper($param_href) ) if $verbose;
    $log->debug( '$param_href = '."$param_print" ) if $verbose;

    #call write modes (different subs that print different jobs)
    my %dispatch = (
        create_db            => \&create_db,              # drop and recreate database in ClickHouse
        import_blastout      => \&import_blastout,        # import BLAST output
        import_map           => \&import_map,             # import Phylostratigraphic map with header
		import_blastdb_stats => \&import_blastdb_stats,   # import BLAST database stats file
		import_names         => \&import_names,           # import names file
		analyze_blastout     => \&analyze_blastout,       # analyzes BLAST output file using mapn names and blastout tables
		report_per_ps        => \&report_per_ps,          # make a report of previous analysis (BLAST hits per phylostratum)
		report_per_ps_unique => \&report_per_ps_unique,   # add unique BLAST hits per species
        import_blastout_full => \&import_blastout_full,   # import BLAST output with all columns
        import_blastdb       => \&import_blastdb,         # import BLAST database with all columns

    );

    foreach my $mode (@mode) {
        if ( exists $dispatch{$mode} ) {
            $log->info("RUNNING ACTION for mode: ", $mode);

            $dispatch{$mode}->( $param_href );

            $log->info("TIME when finished for: $mode");
        }
        else {
            #complain if mode misspelled or just plain wrong
            $log->logcroak( "Unrecognized mode --mode={$mode} on command line thus aborting");
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
	my ($volume, $dir_out, $perl_script) = splitpath( $0 );
	$dir_out = rel2abs($dir_out);
    my ($app_name) = $perl_script =~ m{\A(.+)\.(?:.+)\z};
	$app_name = lc $app_name;
    my $config_file = catfile($volume, $dir_out, $app_name . '.cnf' );
	$config_file = canonpath($config_file);

	#read config to setup defaults
	read_config($config_file => my %config);
	#p(%config);
	my $config_ps_href = $config{PS};
	#p($config_ps_href);
	my $config_ti_href = $config{TI};
	#p($config_ti_href);
	my $config_psname_href = $config{PSNAME};

	#push all options into one hash no matter the section
	my %opts;
	foreach my $key (keys %config) {
		# don't expand PS, TI or PSNAME
		next if ( ($key eq 'PS') or ($key eq 'TI') or ($key eq 'PSNAME') );
		# expand all other options
		%opts = (%opts, %{ $config{$key} });
	}

	# put config location to %opts
	$opts{config} = $config_file;

	# put PS and TI section to %opts
	$opts{ps} = $config_ps_href;
	$opts{ti} = $config_ti_href;
	$opts{psname} = $config_psname_href;

	#cli part
	my @arg_copy = @ARGV;
	my (%cli, @mode);
	$cli{quiet} = 0;
	$cli{verbose} = 0;
	$cli{argv} = \@arg_copy;

	#mode, quiet and verbose can only be set on command line
    GetOptions(
        'help|h'        => \$cli{help},
        'man|m'         => \$cli{man},
        'config|cnf=s'  => \$cli{config},
        'in|i=s'        => \$cli{in},
        'infile|if=s'   => \$cli{infile},
        'out|o=s'       => \$cli{out},
        'outfile|of=s'  => \$cli{outfile},

        'nodes|no=s'    => \$cli{nodes},
        'names|na=s'    => \$cli{names},
		'blastout=s'    => \$cli{blastout},
		'blastout_analysis=s' => \$cli{blastout_analysis},
		'map=s'         => \$cli{map},
		'analyze_ps=s'  => \$cli{analyze_ps},
		'analyze_genomes=s' => \$cli{analyze_genomes},
		'report_per_ps=s' => \$cli{report_per_ps},
        'tax_id|ti=i'   => \$cli{tax_id},

        'host|ho=s'      => \$cli{host},
        'database|d=s'  => \$cli{database},
        'user|u=s'      => \$cli{user},
        'password|p=s'  => \$cli{password},
        'port|po=i'     => \$cli{port},
        'socket|s=s'    => \$cli{socket},

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
	die 'No mode specified on command line' unless $cli{mode};   #DIES here if without mode
	
	#if not -q or --quiet print all this (else be quiet)
	if ($cli{quiet} == 0) {
		#print STDERR 'My @ARGV: {', join( "} {", @arg_copy ), '}', "\n";
		#no warnings 'uninitialized';
		#print STDERR "Extra options from config:", Dumper(\%opts);
	
		if ($cli{in}) {
			say 'My input path: ', canonpath($cli{in});
			$cli{in} = rel2abs($cli{in});
			$cli{in} = canonpath($cli{in});
			say "My absolute input path: $cli{in}";
		}
		if ($cli{infile}) {
			say 'My input file: ', canonpath($cli{infile});
			$cli{infile} = rel2abs($cli{infile});
			$cli{infile} = canonpath($cli{infile});
			say "My absolute input file: $cli{infile}";
		}
		if ($cli{out}) {
			say 'My output path: ', canonpath($cli{out});
			$cli{out} = rel2abs($cli{out});
			$cli{out} = canonpath($cli{out});
			say "My absolute output path: $cli{out}";
		}
		if ($cli{outfile}) {
			say 'My outfile: ', canonpath($cli{outfile});
			$cli{outfile} = rel2abs($cli{outfile});
			$cli{outfile} = canonpath($cli{outfile});
			say "My absolute outfile: $cli{outfile}";
		}
	}
	else {
		$cli{verbose} = -1;   #and logging is OFF

		if ($cli{in}) {
			$cli{in} = rel2abs($cli{in});
			$cli{in} = canonpath($cli{in});
		}
		if ($cli{infile}) {
			$cli{infile} = rel2abs($cli{infile});
			$cli{infile} = canonpath($cli{infile});
		}
		if ($cli{out}) {
			$cli{out} = rel2abs($cli{out});
			$cli{out} = canonpath($cli{out});
		}
		if ($cli{outfile}) {
			$cli{outfile} = rel2abs($cli{outfile});
			$cli{outfile} = canonpath($cli{outfile});
		}
	}

    #copy all config opts
	my %all_opts = %opts;
	#update with cli options
	foreach my $key (keys %cli) {
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
# Usage      : --mode=create_db
# Purpose    : creates database in ClickHouse
# Returns    : nothing
# Parameters : ( $param_href ) -> params from command line to connect to ClickHouse
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

    #first report what are you doing
    $log->info("---------->{$database} database creation");

    # drop database
    my ( $success_del, $res_del ) = _http_exec_query( { query => $query_del, %$param_href } );
    $log->error("Action: dropping $database failed!") unless $success_del;
    $log->debug("Action: database $database dropped successfully!") if $success_del;

    # create database
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Action: creating $database failed!") unless $success_create;
    $log->error("Action: database $database created successfully!") if $success_create;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastout
# Purpose    : imports BLAST output to ClickHouse database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   :
# See Also   :
sub import_blastout {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_blastout() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{infile} or $log->logcroak('no $infile specified on command line!');
    my $table = path($infile)->basename;
    $table =~ s/\./_/g;    #for files that have dots in name
    $table =~ s/_gz//g;    #for files that have dots in name

    # drop and recreate table where we are importing
    my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$table};
    my ( $success_drop, $res_drop ) = _http_exec_query( { query => $query_drop, %$param_href } );
    $log->error("Error: dropping $table failed!") unless $success_drop;
    $log->debug("Action: table $table dropped successfully!") if $success_drop;

    my $columns
      = q{(prot_id String, blast_hit String, perc_id Float32, alignment_length UInt32, mismatches UInt32, gap_openings UInt32, query_start UInt32, query_end UInt32, subject_start UInt32, subject_end UInt32, e_value Float64, bitscore Float32, date Date DEFAULT today())};
    my $engine       = q{ENGINE=MergeTree(date, prot_id, 8192)};
    my $query_create = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$table $columns $engine};
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Error: creating $table failed!") unless $success_create;
    $log->debug("Action: table $table created successfully!") if $success_create;

    # import into table (in ClickHouse) from gzipped file (needs pigz)
    my $import_query
      = qq{INSERT INTO $param_href->{database}.$table (prot_id, blast_hit, perc_id, alignment_length, mismatches, gap_openings, query_start, query_end, subject_start, subject_end, e_value, bitscore) FORMAT TabSeparated};
    my $import_cmd = qq{ pigz -c -d $infile | clickhouse-client --query "$import_query"};
    my ( $stdout, $stderr, $exit ) = _capture_output( $import_cmd, $param_href );
    if ( $exit == 0 ) {
        $log->debug("Action: import to $param_href->{database}.$table success!");
    }
    else {
        $log->error("Error: $import_cmd failed: $stderr");
    }

    # check number of rows inserted
    my $query_cnt = qq{SELECT count() FROM $param_href->{database}.$table};
    my ( $success_cnt, $res_cnt ) = _http_exec_query( { query => $query_cnt, %$param_href } );
	$res_cnt =~ s/\n//g;   # remove trailing newline
    $log->debug("Error: counting rows in $table failed!") unless $success_cnt;
    $log->info("Action: inserted $res_cnt rows into {$table}") if $success_cnt;

    return;
}

### INTERNAL UTILITY ###
# Usage      : --mode=import_map on command line
# Purpose    : imports map with header format and psname (.phmap_names)
# Returns    : nothing
# Parameters : full path to map file and database connection parameters
# Throws     : croaks if wrong number of parameters
# Comments   : creates temp files without header for final load
# See Also   :
sub import_map {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_map() needs {$param_href}') unless @_ == 1;
    my ($param_href) = @_;

    # check required parameters
    if ( !exists $param_href->{infile} ) { $log->logcroak('no $infile specified on command line!'); }

    # get name of map table
    my $map_tbl = path( $param_href->{infile} )->basename;
    ($map_tbl) = $map_tbl =~ m/\A([^\.]+)\.phmap_names\z/;
    $map_tbl .= '_map';

    # create tmp filename in same dir as input map with header
    my $temp_map = path( path( $param_href->{infile} )->parent, $map_tbl );
    open( my $tmp_fh, ">", $temp_map ) or $log->logdie("Error: can't open map $temp_map for writing:$!");

    # need to skip header
    open( my $map_fh, "<", $param_href->{infile} )
      or $log->logdie("Error: can't open map $param_href->{infile} for reading:$!");
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

    # drop and recreate table where we are importing
    my $query_drop = qq{DROP TABLE IF EXISTS $param_href->{database}.$map_tbl};
    my ( $success_drop, $res_drop ) = _http_exec_query( { query => $query_drop, %$param_href } );
    $log->error("Error: dropping $map_tbl failed!") unless $success_drop;
    $log->debug("Action: table $map_tbl dropped successfully!") if $success_drop;

    my $columns      = q{(prot_id String, ps UInt8, ti UInt32, psname String, date Date DEFAULT today())};
    my $engine       = q{ENGINE=MergeTree(date, prot_id, 8192)};
    my $query_create = qq{CREATE TABLE IF NOT EXISTS  $param_href->{database}.$map_tbl $columns $engine};
    my ( $success_create, $res_create ) = _http_exec_query( { query => $query_create, %$param_href } );
    $log->error("Error: creating $map_tbl failed!") unless $success_create;
    $log->debug("Action: table $map_tbl created successfully!") if $success_create;

    # import into $map_tbl
    my $import_query = qq{INSERT INTO $param_href->{database}.$map_tbl (prot_id, ps, ti, psname) FORMAT TabSeparated};
    my $import_cmd   = qq{ cat $temp_map | clickhouse-client --query "$import_query"};
    my ( $stdout, $stderr, $exit ) = _capture_output( $import_cmd, $param_href );
    if ( $exit == 0 ) {
        $log->debug("Action: import to $param_href->{database}.$map_tbl success!");
    }
    else {
        $log->error("Error: $import_cmd failed: $stderr");
    }

    # check number of rows inserted
    my $query_cnt = qq{SELECT count() FROM $param_href->{database}.$map_tbl};
    my ( $success_cnt, $res_cnt ) = _http_exec_query( { query => $query_cnt, %$param_href } );
    $res_cnt =~ s/\n//g;    # remove trailing newline
    $log->debug("Error: counting rows for $map_tbl failed!") unless $success_cnt;
    $log->info("Action: inserted $res_cnt rows into {$map_tbl}") if $success_cnt;

    # unlink tmp map file
    unlink $temp_map and $log->warn("Action: $temp_map unlinked");

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastdb_stats
# Purpose    : import BLAST db stats created by AnalyzePhyloDb
# Returns    : nothing
# Parameters : infile and connection paramaters
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub import_blastdb_stats {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_blastdb_stats() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{infile} or $log->logcroak('no $infile specified on command line!');
	my $stats_ps_tbl = path($infile)->basename;
	$stats_ps_tbl   .= '_stats_ps';
	my $stats_genomes_tbl = path($infile)->basename;
	$stats_genomes_tbl   .='_stats_genomes';

	my $dbh = _http_exec_query($param_href);

    # create ps summary table
    my $ps_summary = sprintf( qq{
	CREATE TABLE %s (
	phylostrata TINYINT UNSIGNED NOT NULL,
	num_of_genomes INT UNSIGNED NOT NULL,
	ti INT UNSIGNED NOT NULL,
	PRIMARY KEY(phylostrata),
	KEY(ti),
	KEY(num_of_genomes)
    ) }, $dbh->quote_identifier($stats_ps_tbl) );
	_create_table( { table_name => $stats_ps_tbl, dbh => $dbh, query => $ps_summary } );
	$log->trace("Report: $ps_summary");

	# create genomes per phylostrata table
    my $genomes_per_ps = sprintf( qq{
	CREATE TABLE %s (
	phylostrata TINYINT UNSIGNED NOT NULL,
	psti INT UNSIGNED NOT NULL,
	num_of_genes INT UNSIGNED NOT NULL,
	ti INT UNSIGNED NOT NULL,
	PRIMARY KEY(ti),
	KEY(phylostrata),
	KEY(num_of_genes)
    ) }, $dbh->quote_identifier($stats_genomes_tbl) );
	_create_table( { table_name => $stats_genomes_tbl, dbh => $dbh, query => $genomes_per_ps } );
	$log->trace("Report: $genomes_per_ps");

	# create tmp file for genomes part of stats file
	my $temp_stats = path(path($infile)->parent, $stats_genomes_tbl);
	open (my $tmp_fh, ">", $temp_stats) or $log->logdie("Error: can't open map $temp_stats for writing:$!");

	# read and import stats file into ClickHouse
	_read_stats_file( { ps_tbl => $stats_ps_tbl, dbh => $dbh, %{$param_href}, tmp_fh => $tmp_fh } );

	# load genomes per phylostrata
    my $load_query = qq{
    LOAD DATA INFILE '$temp_stats'
    INTO TABLE $stats_genomes_tbl } . q{ FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    };
	$log->trace("Report: $load_query");
	my $rows;
    eval { $rows = $dbh->do( $load_query ) };
	$log->error( "Action: loading into table $stats_genomes_tbl failed: $@" ) if $@;
	$log->debug( "Action: table $stats_genomes_tbl inserted $rows rows!" ) unless $@;

	# unlink tmp map file
	unlink $temp_stats and $log->warn("Action: $temp_stats unlinked");
	$dbh->disconnect;

    return;
}


### INTERNAL UTILITY ###
# Usage      : _read_stats_file( { ps_tbl => $stats_ps_tbl, dbh => $dbh, %{$param_href}, tmp_fh => $tmp_fh } );
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

	# prepare statement handle to insert ps lines
	my $insert_ps = sprintf( qq{
	INSERT INTO %s (phylostrata, num_of_genomes, ti)
	VALUES (?, ?, ?)
	}, $p_href->{dbh}->quote_identifier($p_href->{ps_tbl}) );
	my $sth = $p_href->{dbh}->prepare($insert_ps);
	$log->trace("Report: $insert_ps");

	# prepare statement handle to update ps lines
	my $update_ps = sprintf( qq{
	UPDATE %s 
	SET num_of_genomes = num_of_genomes + ?
	WHERE phylostrata = ?
	}, $p_href->{dbh}->quote_identifier($p_href->{ps_tbl}) );
	my $sth_up = $p_href->{dbh}->prepare($update_ps);
	$log->trace("Report: $update_ps");

	# read and import ps_table
	open (my $stats_fh, "<", $p_href->{infile}) or $log->logdie("Error: can't open map $p_href->{infile} for reading:$!");
	while (<$stats_fh>) {
		chomp;

		# if ps then summary line
		if (m/ps/) {
			#import to stats_ps_tbl
			my (undef, $ps, $num_of_genomes, $ti, ) = split "\t", $_;

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
			
			# if it fails (ps already exists) update num_of_genomes
			eval {$sth->execute($ps, $num_of_genomes, $ti); };
			if ($@) {
				$sth_up->execute($num_of_genomes, $ps);
			}
		}
		# else normal genome in phylostrata line
		else {
			my ($ps2, $psti, $num_of_genes, $ti2) = split "\t", $_;

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
			say { $p_href->{tmp_fh} } "$ps2\t$psti\t$num_of_genes\t$ti2";
		}
	}   # end while reading stats file

	# explicit close needed else it can break
	close $p_href->{tmp_fh};
	$sth->finish;
	$sth_up->finish;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_names
# Purpose    : loads names file to ClickHouse
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : new format
# See Also   :
sub import_names {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak ('import_names() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{infile} or $log->logcroak('no $infile specified on command line!');
    my $names_tbl = path($infile)->basename;
    $names_tbl =~ s/\./_/g;    #for files that have dots in name)

    # get new handle
    my $dbh = _http_exec_query($param_href);

    # create names table
    my $create_names = sprintf( qq{
    CREATE TABLE %s (
    id INT UNSIGNED AUTO_INCREMENT NOT NULL,
    ti INT UNSIGNED NOT NULL,
    species_name VARCHAR(200) NOT NULL,
    PRIMARY KEY(id),
    KEY(ti),
    KEY(species_name)
    )}, $dbh->quote_identifier($names_tbl) );
	_create_table( { table_name => $names_tbl, dbh => $dbh, query => $create_names } );
	$log->trace("Report: $create_names");

    #import table
	my $column_list = 'ti, species_name, @dummy, @dummy';
	_load_table_into($names_tbl, $infile, $dbh, $column_list);

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=analyze_blastout
# Purpose    : is to create expanded table per phylostrata with ps, prot_id, ti, species_name
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub analyze_blastout {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('analyze_blastout() needs a $param_href') unless @_ == 1;
    my ($p_href) = @_;

    # get new handle
    my $dbh = _http_exec_query($p_href);

    # create blastout_analysis table
    my $blastout_analysis = sprintf( qq{
    CREATE TABLE %s (
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
	ps TINYINT UNSIGNED NOT NULL,
	prot_id VARCHAR(40) NOT NULL,
	ti INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NULL,
	PRIMARY KEY(id),
	KEY(ti),
	KEY(prot_id)
	)}, $dbh->quote_identifier($p_href->{blastout_analysis}) );
	_create_table( { table_name => $p_href->{blastout_analysis}, dbh => $dbh, query => $blastout_analysis } );
	$log->trace("Report: $blastout_analysis");

#	# create blastout_analysis_all table
#    my $blastout_analysis_all = sprintf( qq{
#    CREATE TABLE %s (
#	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
#	ps TINYINT UNSIGNED NOT NULL,
#	prot_id VARCHAR(40) NOT NULL,
#	ti INT UNSIGNED NOT NULL,
#	species_name VARCHAR(200) NULL,
#	PRIMARY KEY(id),
#	KEY(ti),
#	KEY(prot_id)
#	)}, $dbh->quote_identifier("$p_href->{blastout_analysis}_all") );
#	_create_table( { table_name => "$p_href->{blastout_analysis}_all", dbh => $dbh, query => $blastout_analysis_all } );
#	$log->trace("Report: $blastout_analysis_all");

    # get columns from MAP table to iterate on phylostrata
	my $select_ps_from_map = sprintf( qq{
	SELECT DISTINCT phylostrata FROM %s ORDER BY phylostrata
	}, $dbh->quote_identifier($p_href->{map}) );
	
	# get column phylostrata to array to iterate insert query on them
	my @ps = map { $_->[0] } @{ $dbh->selectall_arrayref($select_ps_from_map) };
	$log->trace( 'Returned phylostrata: {', join('}{', @ps), '}' );
	
	# to insert blastout_analysis and blastout_analysis_all table
	_insert_blastout_analysis( { dbh => $dbh, phylostrata => \@ps, %{$p_href} } );
	
    $dbh->disconnect;
    return;
}


### INTERNAL UTILITY ###
# Usage      : _insert_blastout_analysis( { dbh => $dbh, pphylostrata => \@ps, %{$p_href} } );
# Purpose    : to insert blastout_analysis and blastout_analysis_all table
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : part of --mode=blastout_analyze
# See Also   : 
sub _insert_blastout_analysis {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('_insert_blastout_analysis() needs a $param_href') unless @_ == 1;
    my ($p_href) = @_;

#	# create insert query for each phylostratum (blastout_analysis_all table)
#	my $insert_ps_query_all = qq{
#	INSERT INTO $p_href->{blastout_analysis}_all (ps, prot_id, ti, species_name)
#		SELECT DISTINCT map.phylostrata, map.prot_id, blout.ti, na.species_name
#		FROM $p_href->{blastout} AS blout
#		INNER JOIN $p_href->{map} AS map ON blout.prot_id = map.prot_id
#		INNER JOIN $p_href->{names} AS na ON blout.ti = na.ti
#		WHERE map.phylostrata = ?
#	};
#	my $sth_all = $p_href->{dbh}->prepare($insert_ps_query_all);
#	$log->trace("Report: $insert_ps_query_all");
#	
#	#iterate for each phylostratum and insert into blastout_analysis_all
#	foreach my $ps (@{ $p_href->{phylostrata} }) {
#	    eval { $sth_all->execute($ps) };
#		my $rows = $sth_all->rows;
#	    $log->error( qq{Error: inserting into "$p_href->{blastout_analysis}_all" failed for ps:$ps: $@} ) if $@;
#	    $log->debug( qq{Action: table "$p_href->{blastout_analysis}_all" for ps:$ps inserted $rows rows} ) unless $@;
#	}

	# create insert query for each phylostratum (blastout_analysis table)
	my $insert_ps_query = qq{
	INSERT INTO $p_href->{blastout_analysis} (ps, prot_id, ti, species_name)
		SELECT DISTINCT map.phylostrata, map.prot_id, blout.ti, na.species_name
		FROM $p_href->{blastout} AS blout
		INNER JOIN $p_href->{map} AS map ON blout.prot_id = map.prot_id
		INNER JOIN $p_href->{names} AS na ON blout.ti = na.ti
		INNER JOIN $p_href->{analyze_genomes} AS an ON blout.ti = an.ti
		WHERE map.phylostrata = ? AND an.phylostrata = ?
	};
	my $sth = $p_href->{dbh}->prepare($insert_ps_query);
	$log->trace("Report: $insert_ps_query");
	
	#iterate for each phylostratum and insert into blastout_analysis
	foreach my $ps (@{ $p_href->{phylostrata} }) {
	    eval { $sth->execute($ps, $ps) };
	    my $rows = $sth->rows;
	    $log->error( qq{Error: inserting into $p_href->{blastout_analysis} failed for ps:$ps: $@} ) if $@;
	    $log->debug( qq{Action: table $p_href->{blastout_analysis} for ps:$ps inserted $rows rows} ) unless $@;
	}

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=report_per_ps
# Purpose    : reports blast output analysis per species (ti) per phylostrata
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub report_per_ps {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('report_per_ps() needs a $p_href') unless @_ == 1;
    my ($p_href) = @_;

    my $dbh = _http_exec_query($p_href);

	# name the report_per_ps table
	my $report_per_ps_tbl = "$p_href->{report_per_ps}";

	# create summary per phylostrata per species
    my $report_per_ps = sprintf( qq{
    CREATE TABLE %s (
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
	ps TINYINT UNSIGNED NOT NULL,
	ti INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NULL,
	gene_hits_per_species INT UNSIGNED NOT NULL,
	gene_list MEDIUMTEXT NOT NULL,
	PRIMARY KEY(id),
	KEY(species_name)
	)}, $dbh->quote_identifier($report_per_ps_tbl) );
	_create_table( { table_name => $report_per_ps_tbl, dbh => $dbh, query => $report_per_ps } );
	$log->trace("Report: $report_per_ps");

	#for large GROUP_CONCAT selects
	my $value = 16_777_215;
	my $variables_query = qq{
	SET SESSION group_concat_max_len = $value
	};
	eval { $dbh->do($variables_query) };
    $log->error( "Error: changing SESSION group_concat_max_len=$value failed: $@" ) if $@;
    $log->debug( "Report: changing SESSION group_concat_max_len=$value succeeded" ) unless $@;

	# create insert query
	my $insert_report_per_ps = sprintf( qq{
		INSERT INTO %s (ps, ti, species_name, gene_hits_per_species, gene_list)
		SELECT ps, ti, species_name, COUNT(species_name) AS gene_hits_per_species, 
		GROUP_CONCAT(prot_id ORDER BY prot_id) AS gene_list
		FROM %s
		GROUP BY species_name
		ORDER BY ps, gene_hits_per_species, species_name
	}, $dbh->quote_identifier($report_per_ps_tbl), $dbh->quote_identifier($p_href->{blastout_analysis}) );
	my $rows;
	eval { $rows = $dbh->do($insert_report_per_ps) };
    $log->error( "Error: inserting into $report_per_ps_tbl failed: $@" ) if $@;
    $log->debug( "Action: table $report_per_ps_tbl inserted $rows rows" ) unless $@;
	$log->trace("$insert_report_per_ps");

    $dbh->disconnect;

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=report_per_ps_unique
# Purpose    : reports blast output analysis per species (ti) per phylostrata and unique hits
# Returns    : nothing
# Parameters : 
# Throws     : croaks if wrong number of parameters
# Comments   : 
# See Also   : 
sub report_per_ps_unique {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('report_per_ps_unique() needs a $p_href') unless @_ == 1;
    my ($p_href) = @_;

    my $out = $p_href->{out} or $log->logcroak('no $out specified on command line!');
    my $dbh = _http_exec_query($p_href);

	# name the report_per_ps table
	my $report_per_ps_tbl = "$p_href->{report_per_ps}";

	# create summary per phylostrata per species
    my $report_per_ps_alter = sprintf( qq{
    ALTER TABLE %s ADD COLUMN hits1 INT, ADD COLUMN hits2 INT, ADD COLUMN hits3 INT, ADD COLUMN hits4 INT, ADD COLUMN hits5 INT, 
	ADD COLUMN hits6 INT, ADD COLUMN hits7 INT, ADD COLUMN hits8 INT, ADD COLUMN hits9 INT, ADD COLUMN hits10 INT, 
	ADD COLUMN list1 MEDIUMTEXT, ADD COLUMN list2 MEDIUMTEXT, ADD COLUMN list3 MEDIUMTEXT, ADD COLUMN list4 MEDIUMTEXT, ADD COLUMN list5 MEDIUMTEXT,
	ADD COLUMN list6 MEDIUMTEXT, ADD COLUMN list7 MEDIUMTEXT, ADD COLUMN list8 MEDIUMTEXT, ADD COLUMN list9 MEDIUMTEXT, ADD COLUMN list10 MEDIUMTEXT
	}, $dbh->quote_identifier($report_per_ps_tbl) );
	$log->trace("Report: $report_per_ps_alter");
	eval { $dbh->do($report_per_ps_alter) };
    $log->error( "Error: table $report_per_ps_tbl failed to alter: $@" ) if $@;
    $log->debug( "Report: table $report_per_ps_tbl alter succeeded" ) unless $@;

	#for large GROUP_CONCAT selects
	my $value = 16_777_215;
	my $variables_query = qq{
	SET SESSION group_concat_max_len = $value
	};
	eval { $dbh->do($variables_query) };
    $log->error( "Error: changing SESSION group_concat_max_len=$value failed: $@" ) if $@;
    $log->debug( "Report: changing SESSION group_concat_max_len=$value succeeded" ) unless $@;

    # get columns from REPORT_PER_PS table to iterate on phylostrata
	my $select_ps = sprintf( qq{
	SELECT DISTINCT ps FROM %s ORDER BY ps
	}, $dbh->quote_identifier($report_per_ps_tbl) );
	
	# get column phylostrata to array to iterate insert query on them
	my @ps = map { $_->[0] } @{ $dbh->selectall_arrayref($select_ps) };
	$log->trace( 'Returned phylostrata: {', join('}{', @ps), '}' );

	# prepare insert query
	my $ins_hits = sprintf( qq{
	UPDATE %s
	SET hits1 = ?, hits2 = ?, hits3 = ?, hits4 = ?, hits5 = ?, hits6 = ?, hits7 = ?, hits8 = ?, hits9 = ?, hits10 = ?, 
	list1 = ?, list2 = ?, list3 = ?, list4 = ?, list5 = ?, list6 = ?, list7 = ?, list8 = ?, list9 = ?, list10 = ?
	WHERE ti = ?
	}, $dbh->quote_identifier($report_per_ps_tbl) );
	my $sth = $dbh->prepare($ins_hits);

	# insert hits and genelists into database
	foreach my $ps (@ps) {

		#get gene_list from db
		my $select_gene_list_from_report = sprintf( qq{
	    SELECT DISTINCT ti, gene_list
		FROM %s
		WHERE ps = $ps
		ORDER BY gene_hits_per_species
	    }, $dbh->quote_identifier($report_per_ps_tbl) );
	    my %ti_genelist_h = map { $_->[0], $_->[1]} @{$dbh->selectall_arrayref($select_gene_list_from_report)};

		# get ti list sorted by gene_hits_per_species
		my @ti = map { $_->[0] } @{ $dbh->selectall_arrayref($select_gene_list_from_report) };

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
		foreach my $ti (@ti) {
			my @ti_genelist = @{ $ti_genelist_h{$ti} };
			my ($ti_unique, $ti2, $ti3, $ti4, $ti5, $ti6, $ti7, $ti8, $ti9, $ti10) = (0) x 11;
			my ($ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g) = ('') x 11;

			# do the calculation here (tabulated ternary) 10 and 10+hits go to hits10
			foreach my $prot_id (@ti_genelist) {
				$gene_count{$prot_id} == 1 ? do {$ti_unique++; $ti_uniq_genes .= ',' . $prot_id;} : 
				$gene_count{$prot_id} == 2 ? do {$ti2++; $ti2g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 3 ? do {$ti3++; $ti3g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 4 ? do {$ti4++; $ti4g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 5 ? do {$ti5++; $ti5g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 6 ? do {$ti6++; $ti6g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 7 ? do {$ti7++; $ti7g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 8 ? do {$ti8++; $ti8g .= ',' . $prot_id;}                : 
				$gene_count{$prot_id} == 9 ? do {$ti9++; $ti9g .= ',' . $prot_id;}                : 
				                                                                                    do {$ti10++; $ti10g .= ',' . $prot_id;};
			}

			# remove comma at start
			foreach my $genelist ($ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g) {
				$genelist =~ s/\A,(.+)\z/$1/;
			}

			# insert into db
			$sth->execute($ti_unique, $ti2, $ti3, $ti4, $ti5, $ti6, $ti7, $ti8, $ti9, $ti10, $ti_uniq_genes, $ti2g, $ti3g, $ti4g, $ti5g, $ti6g, $ti7g, $ti8g, $ti9g, $ti10g, $ti);
			#say "TI:$ti\tuniq:$ti_unique\tti2:$ti2\tti3:$ti3\tti4:$ti4\tti5:$ti5";
			#say "TI:$ti\tuniq:$ti_uniq_genes\tti2:$ti2g\tti3:$ti3g\tti4:$ti4g\tti5:$ti5g";
		}

		$log->debug("Report: inserted ps $ps");
	}   # end foreach ps
	
	#export to tsv file
	my $out_report_per_ps = path($out, $report_per_ps_tbl);
	if (-f $out_report_per_ps ) {
		unlink $out_report_per_ps and $log->warn( "Warn: file $out_report_per_ps found and unlinked" );
	}
	else {
		$log->trace( "Action: file $out_report_per_ps will be created by SELECT INTO OUTFILE" );
	}
	my $export_report_per_ps = qq{
		SELECT * FROM $report_per_ps_tbl
		INTO OUTFILE '$out_report_per_ps' } 
		. q{
		FIELDS TERMINATED BY '\t'
		LINES TERMINATED BY '\n';
	};

	my $r_ex;
    eval { $r_ex = $dbh->do($export_report_per_ps) };
    $log->error( "Error: exporting $report_per_ps_tbl to $out_report_per_ps failed: $@" ) if $@;
    $log->debug( "Action: table $report_per_ps_tbl exported $r_ex rows to $out_report_per_ps" ) unless $@;

	$sth->finish;
    $dbh->disconnect;

    return;
}



### INTERFACE SUB ###
# Usage      : --mode=import_blastout_full
# Purpose    : loads full BLAST output to ClickHouse database (no duplicates)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it removes duplicates (same tax_id) per gene
# See Also   : utility sub _extract_blastout()
sub import_blastout_full {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'import_blastout_full() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{infile} or $log->logcroak('no $infile specified on command line!');
    my $table           = path($infile)->basename;
    $table =~ s/\./_/g;    #for files that have dots in name
    my $blastout_import = path($infile . "_formated");

    #first shorten the blastout file and extract useful columns
    _extract_blastout_full( { infile => $infile, blastout_import => $blastout_import } );

    #get new handle
    my $dbh = _http_exec_query($param_href);

    #create table
    my $create_query = qq{
    CREATE TABLE IF NOT EXISTS $table (
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
    prot_id VARCHAR(40) NOT NULL,
    ti INT UNSIGNED NOT NULL,
    pgi CHAR(19) NOT NULL,
    hit VARCHAR(40) NOT NULL,
    col3 FLOAT NOT NULL,
    col4 INT UNSIGNED NOT NULL,
    col5 INT UNSIGNED NOT NULL,
    col6 INT UNSIGNED NOT NULL,
    col7 INT UNSIGNED NOT NULL,
    col8 INT UNSIGNED NOT NULL,
    col9 INT UNSIGNED NOT NULL,
    col10 INT UNSIGNED NOT NULL,
    evalue REAL NOT NULL,
	bitscore FLOAT NOT NULL,
	PRIMARY KEY(id)
    )};
    _create_table( { table_name => $table, dbh => $dbh, query => $create_query } );

    #import table
    my $load_query = qq{
    LOAD DATA INFILE '$blastout_import'
    INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n' 
    (prot_id, ti, pgi, hit, col3, col4, col5, col6, col7, col8, col9, col10, evalue, bitscore)
    };
	$log->trace("$load_query");
    eval { $dbh->do( $load_query, { async => 1 } ) };

    # check status while running
    my $dbh_check             = _http_exec_query($param_href);
    until ( $dbh->mysql_async_ready ) {
        my $processlist_query = qq{
        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
        };
        my ( $time, $state );
        my $sth = $dbh_check->prepare($processlist_query);
        $sth->execute($param_href->{database});
        $sth->bind_columns( \( $time, $state ) );
        while ( $sth->fetchrow_arrayref ) {
            my $print = sprintf( "Time running:%d sec\tSTATE:%s\n", $time, $state );
            $log->trace( $print );
            sleep 10;
        }
    }
    my $rows;
	eval { $rows = $dbh->mysql_async_result; };
    $log->info( "Action: import inserted $rows rows!" ) unless $@;
    $log->error( "Error: loading $table failed: $@" ) if $@;

    # add index
    my $alter_query = qq{
    ALTER TABLE $table ADD INDEX protx(prot_id), ADD INDEX tix(ti)
    };
    eval { $dbh->do( $alter_query, { async => 1 } ) };

    # check status while running
    my $dbh_check2            = _http_exec_query($param_href);
    until ( $dbh->mysql_async_ready ) {
        my $processlist_query = qq{
        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE DB = ? AND INFO LIKE 'ALTER%';
        };
        my ( $time, $state );
        my $sth = $dbh_check2->prepare($processlist_query);
        $sth->execute($param_href->{database});
        $sth->bind_columns( \( $time, $state ) );
        while ( $sth->fetchrow_arrayref ) {
            my $print = sprintf( "Time running:%d sec\tSTATE:%s\n", $time, $state );
            $log->trace( $print );
            sleep 10;
        }
    }

    #report success or failure
    $log->error( "Error: adding index tix on $table failed: $@" ) if $@;
    $log->info( "Action: Indices protx and tix on $table added successfully!" ) unless $@;
	
	#delete file used to import so it doesn't use disk space
	#unlink $blastout_import and $log->warn("File $blastout_import unlinked!");

    return;
}

### INTERNAL UTILITY ###
# Usage      : _extract_blastout_full( { infile => $infile, blastout_import => $blastout_import } );
# Purpose    : removes duplicates per tax_id from blastout file and saves blastout into file
# Returns    : nothing
# Parameters : ($param_href)
# Throws     : croaks for parameters
# Comments   : needed for --mode=import_blastout_full()
# See Also   : import_blastout_full()
sub _extract_blastout_full {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'extract_blastout_full() needs {hash_ref}' ) unless @_ == 1;
    my ($extract_href) = @_;

    open( my $blastout_fh, "< :encoding(ASCII)", $extract_href->{infile} ) or $log->logdie( "Error: BLASTout file not found:$!" );
    open( my $blastout_fmt_fh, "> :encoding(ASCII)", $extract_href->{blastout_import} ) or $log->logdie( "Error: BLASTout file can't be created:$!" );

    # needed for filtering duplicates
    # idea is that duplicates come one after another
    my $prot_prev    = '';
    my $pgi_prev     = 0;
    my $ti_prev      = 0;
	my $formated_cnt = 0;

    # in blastout
    #ENSG00000151914|ENSP00000354508    pgi|34252924|ti|9606|pi|0|  100.00  7461    0   0   1   7461    1   7461    0.0 1.437e+04
    
	$log->debug( "Report: started processing of $extract_href->{infile}" );
    local $.;
    while ( <$blastout_fh> ) {
        chomp;

		my ($prot_id, $hit, $col3, $col4, $col5, $col6, $col7, $col8, $col9, $col10, $evalue, $bitscore) = split "\t", $_;
		my ($pgi, $ti) = $hit =~ m{pgi\|(\d+)\|ti\|(\d+)\|pi\|(?:\d+)\|};

        # check for duplicates for same gene_id with same tax_id and pgi that differ only in e_value
        if (  "$prot_prev" . "$pgi_prev" . "$ti_prev" ne "$prot_id" . "$pgi" . "$ti" ) {
            say {$blastout_fmt_fh} $prot_id, "\t", $ti, "\t", $pgi,  "\t$hit\t$col3\t$col4\t$col5\t$col6\t$col7\t$col8\t$col9\t$col10\t$evalue\t$bitscore";
			$formated_cnt++;
        }

        # set found values for next line to check duplicates
        $prot_prev = $prot_id;
        $pgi_prev  = $pgi;
        $ti_prev   = $ti;

		# show progress
        if ($. % 1000000 == 0) {
            $log->trace( "$. lines processed!" );
        }

    }   # end while reading blastout

    $log->info( "Report: file $extract_href->{blastout_import} printed successfully with $formated_cnt lines (from $. original lines)" );

    return;
}


### INTERFACE SUB ###
# Usage      : --mode=import_blastdb
# Purpose    : loads BLAST database to ClickHouse database from compressed file using named pipe
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : works on compressed file
# See Also   : 
sub import_blastdb {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'import_blastdb() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $infile = $param_href->{infile} or $log->logcroak('no $infile specified on command line!');
    my $table  = path($infile)->basename;
    $table     =~ s/\./_/g;    #for files that have dots in name
	my $out    = path($infile)->parent;

	# get date for named pipe file naming
    my $now  = DateTime::Tiny->now;
    my $date = $now->year . '_' . $now->month . '_' . $now->day . '_' . $now->hour . '_' . $now->minute . '_' . $now->second;
	
	# delete pipe if it exists
	my $load_file = path($out, "blastdb_named_pipe_${date}");   #file for LOAD DATA INFILE
	if (-p $load_file) {
		unlink $load_file and $log->trace( "Action: named pipe $load_file removed!" );
	}
	#make named pipe
	mkfifo( $load_file, 0666 ) or $log->logdie( "Error: mkfifo $load_file failed: $!" );

	# open blastdb compressed file for reading
	open my $blastdb_fh, "<:gzip", $infile or $log->logdie( "Can't open gzipped file $infile: $!" );

	#start 2 processes (one for Perl-child and ClickHouse-parent)
    my $pid = fork;

	if (!defined $pid) {
		$log->logdie( "Error: cannot fork: $!" );
	}

	elsif ($pid == 0) {
		# Child-client process
		$log->warn( "Action: Perl-child-client starting..." );

		# open named pipe for writing (gziped file --> named pipe)
		open my $blastdb_pipe_fh, "+<:encoding(ASCII)", $load_file or die $!;   #+< mode=read and write
		
		# define new block for reading blocks of fasta
		{
			local $/ = ">pgi";  #look in larger chunks between >gi (solo > found in header so can't use)
			local $.;           #gzip count
			my $out_cnt = 0;    #named pipe count

			# print to named pipe
			PIPE:
			while (<$blastdb_fh>) {
				chomp;
				#print $blastdb_pipe_fh "$_";
				#say '{', $_, '}';
				next PIPE if $_ eq '';   #first iteration is empty?
				
				# extract pgi, prot_name and fasta + fasta
				my ($prot_id, $prot_name, $fasta) = $_ =~ m{\A([^\t]+)\t([^\n]+)\n(.+)\z}smx;

				#pgi removed as record separator (return it back)
				$prot_id = 'pgi' . $prot_id;
		        my ($pgi, $ti) = $prot_id =~ m{pgi\|(\d+)\|ti\|(\d+)\|pi\|(?:\d+)\|};

				# remove illegal chars from fasta and upercase it
			    $fasta =~ s/\R//g;      #delete multiple newlines (all vertical and horizontal space)
				$fasta = uc $fasta;     #uppercase fasta
			    $fasta =~ tr{A-Z}{}dc;  #delete all special characters (all not in A-Z)

				# print to pipe
				print {$blastdb_pipe_fh} "$prot_id\t$pgi\t$ti\t$prot_name\t$fasta\n";
				$out_cnt++;

				#progress tracker for blastdb file
				if ($. % 1000000 == 0) {
					$log->trace( "$. lines processed!" );
				}
			}
			my $blastdb_file_line_cnt = $. - 1;   #first line read empty (don't know why)
			$log->warn( "Report: file $infile has $blastdb_file_line_cnt fasta records!" );
			$log->warn( "Action: file $load_file written with $out_cnt lines/fasta records!" );
		}   #END block writing to pipe

		$log->warn( "Action: Perl-child-client terminating :)" );
		exit 0;
	}
	else {
		# ClickHouse-parent process
		$log->warn( "Action: ClickHouse-parent process, waiting for child..." );
		
		# SECOND PART: loading named pipe into db
		my $database = $param_href->{database}    or $log->logcroak( 'no $database specified on command line!' );
		
		# get new handle
    	my $dbh = _http_exec_query($param_href);

    	# create a table to load into
    	my $create_query = sprintf( qq{
    	CREATE TABLE %s (
    	prot_id VARCHAR(40) NOT NULL,
        pgi CHAR(19) NOT NULL,
		ti INT UNSIGNED NOT NULL,
    	prot_name VARCHAR(200) NOT NULL,
    	fasta MEDIUMTEXT NOT NULL,
    	PRIMARY KEY(pgi)
    	)}, $dbh->quote_identifier($table) );
		_create_table( { table_name => $table, dbh => $dbh, query => $create_query, %{$param_href} } );
		$log->trace("Report: $create_query");

		#import table
    	my $load_query = qq{
    	LOAD DATA INFILE '$load_file'
    	INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    	LINES TERMINATED BY '\n'
    	};
    	eval { $dbh->do( $load_query, { async => 1 } ) };

    	#check status while running LOAD DATA INFILE
    	{    
    	    my $dbh_check         = _http_exec_query($param_href);
    	    until ( $dbh->mysql_async_ready ) {
				my $processlist_query = qq{
					SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
					WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
					};
    	        my $sth = $dbh_check->prepare($processlist_query);
    	        $sth->execute($database);
    	        my ( $time, $state );
    	        $sth->bind_columns( \( $time, $state ) );
    	        while ( $sth->fetchrow_arrayref ) {
    	            my $process = sprintf( "Time running:%d sec\tSTATE:%s\n", $time, $state );
    	            $log->trace( $process );
    	            sleep 10;
    	        }
    	    }
    	}    #end check LOAD DATA INFILE
    	my $rows = $dbh->mysql_async_result;
    	$log->info( "Report: import inserted $rows rows!" );
    	$log->error( "Report: loading $table failed: $@" ) if $@;

		# add index
	    my $alter_query = qq{
	    ALTER TABLE $table ADD INDEX tix(ti)
	    };
	    eval { $dbh->do( $alter_query, { async => 1 } ) };
	
	    # check status while running
	    my $dbh_check2            = _http_exec_query($param_href);
	    until ( $dbh->mysql_async_ready ) {
	        my $processlist_query = qq{
	        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
	        WHERE DB = ? AND INFO LIKE 'ALTER%';
	        };
	        my ( $time, $state );
	        my $sth = $dbh_check2->prepare($processlist_query);
	        $sth->execute($param_href->{database});
	        $sth->bind_columns( \( $time, $state ) );
	        while ( $sth->fetchrow_arrayref ) {
	            my $print = sprintf( "Time running:%d sec\tSTATE:%s\n", $time, $state );
	            $log->trace( $print );
	            sleep 10;
	        }
	    }
	
	    #report success or failure
	    $log->error( "Error: adding index tix on $table failed: $@" ) if $@;
	    $log->info( "Action: Index tix on $table added successfully!" ) unless $@;

		$dbh->disconnect;

		# communicate with child process
		waitpid $pid, 0;
	}
	$log->warn( "ClickHouse-parent process end after child has finished" );
		unlink $load_file and $log->warn( "Action: named pipe $load_file removed!" );

	return;
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
    FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz

    # remove header and import phylostratigraphic map into ClickHouse database (reads PS, TI and PSNAME from config)
    FindOrigin.pm --mode=import_map -if t/data/hs3.phmap_names -d hs_plus -v

    # imports analyze stats file created by AnalyzePhyloDb (uses TI and PS sections in config)
    FindOrigin.pm --mode=import_blastdb_stats -if t/data/analyze_hs_9606_cdhit_large_extracted  -d hs_plus -v

    # import names file for species_name
    FindOrigin.pm --mode=import_names -if t/data/names.dmp.fmt.new  -d hs_plus -v

    # runs BLAST output analysis - expanding every prot_id to its tax_id hits and species names
    FindOrigin.pm --mode=analyze_blastout -d hs_plus -v

    # runs summary per phylostrata per species of BLAST output analysis.
    FindOrigin.pm --mode=report_per_ps -o -d hs_plus -v

    # removes specific hits from the BLAST output based on the specified tax_id (exclude bad genomes).
    FindOrigin.pm --mode=exclude_ti_from_blastout -if t/data/hs_all_plus_21_12_2015 -ti 428574 -v

    # update report_per_ps table with unique and intersect hts and gene lists
    FindOrigin.pm --mode=report_per_ps_unique -o t/data/ --report_per_ps=hs_all_plus_21_12_2015_report_per_ps -d hs_plus -v

    # import full blastout with all columns (plus ti and pgi)
    FindOrigin.pm --mode=import_blastout -if t/data/hs_all_plus_21_12_2015 -d hs_blastout -v

    # import full BLAST database (plus ti and pgi columns)
    FindOrigin.pm --mode=import_blastdb -if t/data/db90_head.gz -d hs_blastout -v -v


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
 FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz -ho localhost -po 8123 -v

 # options from config
 FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz

Imports compressed BLAST output file (.gz needs pigz) into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
It drops and recreates table where it will import and runs separate query at end to return numer of rows inserted.

=item import_map

 # options from command line
 FindOrigin.pm --mode=import_map -if t/data/hs3.phmap_names -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=import_map -if t/data/hs3.phmap_names -d hs_plus -v

Removes header from map file and writes columns (prot_id, phylostrata, ti, psname) to tmp file and imports that file into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
It can use PS and TI config sections.

=item import_blastdb_stats

 # options from command line
 FindOrigin.pm --mode=import_blastdb_stats -if t/data/analyze_hs_9606_cdhit_large_extracted  -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=import_blastdb_stats -if t/data/analyze_hs_9606_cdhit_large_extracted  -d hs_plus -v

Imports analyze stats file created by AnalyzePhyloDb.
  AnalysePhyloDb -n /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes.dmp.fmt.new.sync -d /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted/ -t 9606 > analyze_hs_9606_cdhit_large_extracted
It can use PS and TI config sections.

=item import_names

 # options from command line
 FindOrigin.pm --mode=import_names -if t/data/names.dmp.fmt.new  -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=import_names -if t/data/names.dmp.fmt.new  -d hs_plus -v

Imports names file (columns ti, species_name) into ClickHouse.

=item analyze_blastout

 # options from command line
 FindOrigin.pm --mode=analyze_blastout -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=analyze_blastout -d hs_plus -v

Runs BLAST output analysis - expanding every prot_id to its tax_id hits and species names. It creates 2 table: one with all tax_ids fora each gene, and one with tax_ids only that are for phylostratum of interest.


=item report_per_ps

 # options from command line
 lib/FindOrigin.pm --mode=report_per_ps -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 lib/FindOrigin.pm --mode=report_per_ps -d hs_plus -v

Runs summary per phylostrata per species of BLAST output analysis.

=item exclude_ti_from_blastout

 # options from command line
 lib/FindOrigin.pm --mode=exclude_ti_from_blastout -if t/data/hs_all_plus_21_12_2015 -ti 428574 -v

 # options from config
 lib/FindOrigin.pm --mode=exclude_ti_from_blastout -if t/data/hs_all_plus_21_12_2015 -ti 428574 -v

Removes specific hits from the BLAST output based on the specified tax_id (exclude bad genomes).

=item report_per_ps_unique

 # options from command line
 FindOrigin.pm --mode=report_per_ps_unique -o t/data/ --report_per_ps=hs_all_plus_21_12_2015_report_per_ps -d hs_plus -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=report_per_ps_unique -d hs_plus -v

Update report_per_ps table with unique and intersect hits and gene lists.

=item import_blastout_full

 # options from command line
 FindOrigin.pm --mode=import_blastout -if t/data/hs_all_plus_21_12_2015 -d hs_blastout -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=import_blastout -if t/data/hs_all_plus_21_12_2015 -d hs_blastout -v

Extracts hit column and splits it on ti and pgi and imports this file into ClickHouse (it has 2 extra columns = ti and pgi with no duplicates). It needs ClickHouse connection parameters to connect to ClickHouse.

 [2016/04/20 16:12:42,230] INFO> FindOrigin::run line:101==>RUNNING ACTION for mode: import_blastout_full
 [2016/04/20 16:12:42,232]DEBUG> FindOrigin::_extract_blastout_full line:1644==>Report: started processing of /home/msestak/prepare_blast/out/random/hs_all_plus_21_12_2015_good
 [2016/04/20 16:12:51,790]TRACE> FindOrigin::_extract_blastout_full line:1664==>1000000 lines processed!
 ...  Perl processing (3.5 h)
 [2016/04/20 19:37:59,376]TRACE> FindOrigin::_extract_blastout_full line:1664==>1151000000 lines processed!
 [2016/04/20 19:38:07,991] INFO> FindOrigin::_extract_blastout_full line:1670==>Report: file /home/msestak/prepare_blast/out/random/hs_all_plus_21_12_2015_good_formated printed successfully with 503625726 lines (from 1151804042 original lines)
 [2016/04/20 19:38:08,034]TRACE> FindOrigin::import_blastout_full line:1575==>Time running:0 sec    STATE:Fetched about 2000 rows, loading data still remains
 ... Load (50 min)
 [2016/04/20 20:28:58,788] INFO> FindOrigin::import_blastout_full line:1581==>Action: import inserted 503625726 rows!
 [2016/04/20 20:28:58,807]TRACE> FindOrigin::import_blastout_full line:1603==>Time running:0 sec    STATE:Adding indexes
 ... indexing (33 min)
 [2016/04/20 21:01:59,155] INFO> FindOrigin::import_blastout_full line:1610==>Action: Indices protx and tix on hs_all_plus_21_12_2015_good added successfully!
 [2016/04/20 21:01:59,156] INFO> FindOrigin::run line:105==>TIME when finished for: import_blastout_full


=item import_blastdb

 # options from command line
 FindOrigin.pm --mode=import_blastdb -if t/data/db90_head.gz -d hs_blastout -v -p msandbox -u msandbox -po 8123

 # options from config
 FindOrigin.pm --mode=import_blastdb -if t/data/db90_head.gz -d hs_blastout -v -v

Imports BLAST database file into ClickHouse (it has 2 extra columns = ti and pgi). It needs ClickHouse connection parameters to connect to ClickHouse.

 ... Load (41 min)
 [2016/04/22 00:41:42,563]TRACE> FindOrigin::import_blastdb line:1815==>Time running:2460 sec       STATE:Verifying index uniqueness: Checked 43450000 of 0 rows in key-PR
 [2016/04/22 00:41:52,564] INFO> FindOrigin::import_blastdb line:1821==>Report: import inserted 43899817 rows!
 [2016/04/22 00:41:52,567]TRACE> FindOrigin::import_blastdb line:1843==>Time running:0 sec  STATE:Adding indexes
 ... Indexing (2 min)
 [2016/04/22 00:43:52,588] INFO> FindOrigin::import_blastdb line:1850==>Action: Index tix on db90_gz added successfully!
 [2016/04/22 00:43:52,590] INFO> FindOrigin::run line:109==>TIME when finished for: import_blastdb

=back

=head1 CONFIGURATION

All configuration in set in blastoutanalyze.cnf that is found in ./lib directory (it can also be set with --config option on command line). It follows L<< Config::Std|https://metacpan.org/pod/Config::Std >> format and rules.
Example:

 [General]
 #in       = /home/msestak/prepare_blast/out/dr_plus/
 #out      = /msestak/gitdir/ClickHouseinstall
 #infile   = /home/msestak/mysql-5.6.27-linux-glibc2.5-x86_64.tar.gz
 #outfile  = /home/msestak/prepare_blast/out/dr_04_02_2016.xlsx
 
 [Database]
 host     = localhost
 database = test_db_here
 user     = msandbox
 password = msandbox
 port     = 8123
 socket   = /tmp/mysql_sandbox8123.sock
 charset  = ascii

=head1 LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Martin Sebastijan Šestak
mocnii
E<lt>msestak@irb.hrE<gt>

=cut
