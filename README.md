# NAME

FindOrigin - It's a modulino used to analyze BLAST output and database in ClickHouse (columnar DBMS for OLAP).

# SYNOPSIS

    # drop and recreate database (connection parameters in blastoutanalyze.cnf)
    FindOrigin.pm --mode=create_db -d test_db_here

    # import BLAST output file into ClickHouse database
    FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz

    # remove header and import phylostratigraphic map into ClickHouse database (reads PS, TI and PSNAME from config)
    FindOrigin.pm --mode=import_map -d jura -if ./t/data/hs3.phmap_names -v

    # imports analyze stats file created by AnalyzePhyloDb (uses TI and PS sections in config)
    FindOrigin.pm --mode=import_blastdb_stats -d jura -if ./t/data/analyze_hs_9606_all_ff_for_db -v

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

# DESCRIPTION

FindOrigin is modulino used to analyze BLAST database (to get content in genomes and sequences) and BLAST output (to figure out where are hits coming from). It includes config, command-line and logging management.

    For help write:
    FindOrigin.pm -h
    FindOrigin.pm -m

## MODES

- create\_db

        # options from command line
        FindOrigin.pm --mode=create_db -ho localhost -d test_db_here -po 8123

        # options from config
        FindOrigin.pm --mode=create_db -d test_db_here

    Drops ( if it exists) and recreates database in ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).

- import\_blastout

        # options from command line
        FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_blastout -d jura -if /msestak/hs_1mil.gz

    Imports compressed BLAST output file (.gz needs pigz) into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
    It drops and recreates table where it will import and runs separate query at end to return numer of rows inserted.

- import\_map

        # options from command line
        FindOrigin.pm --mode=import_map -d jura -if ./t/data/hs3.phmap_names -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_map -d jura -if ./t/data/hs3.phmap_names -v

    Removes header from map file and writes columns (prot\_id, phylostrata, ti, psname) to tmp file and imports that file into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
    It can use PS, TI and PSNAME config sections.

- import\_blastdb\_stats

        # options from command line
        FindOrigin.pm --mode=import_blastdb_stats -d jura -if ./t/data/analyze_hs_9606_all_ff_for_db -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_blastdb_stats -d jura -if ./t/data/analyze_hs_9606_all_ff_for_db -v

    Imports analyze stats file created by AnalyzePhyloDb.
     AnalysePhyloDb -n nr\_raw/nodes.dmp.fmt.new.sync -t 9606 -d ./all\_ff\_for\_db/ > analyze\_hs\_9606\_all\_ff\_for\_db

    It can use PS and TI config sections.

- import\_names

        # options from command line
        FindOrigin.pm --mode=import_names -if t/data/names.dmp.fmt.new  -d hs_plus -v -p msandbox -u msandbox -po 8123

        # options from config
        FindOrigin.pm --mode=import_names -if t/data/names.dmp.fmt.new  -d hs_plus -v

    Imports names file (columns ti, species\_name) into ClickHouse.

- analyze\_blastout

        # options from command line
        FindOrigin.pm --mode=analyze_blastout -d hs_plus -v -p msandbox -u msandbox -po 8123

        # options from config
        FindOrigin.pm --mode=analyze_blastout -d hs_plus -v

    Runs BLAST output analysis - expanding every prot\_id to its tax\_id hits and species names. It creates 2 table: one with all tax\_ids fora each gene, and one with tax\_ids only that are for phylostratum of interest.

- report\_per\_ps

        # options from command line
        lib/FindOrigin.pm --mode=report_per_ps -d hs_plus -v -p msandbox -u msandbox -po 8123

        # options from config
        lib/FindOrigin.pm --mode=report_per_ps -d hs_plus -v

    Runs summary per phylostrata per species of BLAST output analysis.

- exclude\_ti\_from\_blastout

        # options from command line
        lib/FindOrigin.pm --mode=exclude_ti_from_blastout -if t/data/hs_all_plus_21_12_2015 -ti 428574 -v

        # options from config
        lib/FindOrigin.pm --mode=exclude_ti_from_blastout -if t/data/hs_all_plus_21_12_2015 -ti 428574 -v

    Removes specific hits from the BLAST output based on the specified tax\_id (exclude bad genomes).

- report\_per\_ps\_unique

        # options from command line
        FindOrigin.pm --mode=report_per_ps_unique -o t/data/ --report_per_ps=hs_all_plus_21_12_2015_report_per_ps -d hs_plus -v -p msandbox -u msandbox -po 8123

        # options from config
        FindOrigin.pm --mode=report_per_ps_unique -d hs_plus -v

    Update report\_per\_ps table with unique and intersect hits and gene lists.

- import\_blastout\_full

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

- import\_blastdb

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

# CONFIGURATION

All configuration in set in blastoutanalyze.cnf that is found in ./lib directory (it can also be set with --config option on command line). It follows [Config::Std](https://metacpan.org/pod/Config::Std) format and rules.
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

# LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Martin Sebastijan Šestak
mocnii
<msestak@irb.hr>
