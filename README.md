# NAME

FindOrigin - It's a modulino used to analyze BLAST output and database in ClickHouse (columnar DBMS for OLAP).

# SYNOPSIS

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
        FindOrigin.pm --mode=import_blastout -d jura --blastout t/data/hs_all_plus_21_12_2015.gz -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_blastout -d jura --blastout t/data/hs_all_plus_21_12_2015.gz

    Imports compressed BLAST output file (.gz needs pigz) into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
    It drops and recreates table in a database where it will import it and runs separate query at end to return number of rows inserted.

- import\_map

        # options from command line
        FindOrigin.pm --mode=import_map -d jura --map t/data/hs3.phmap_names -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_map -d jura --map t/data/hs3.phmap_names -v

    Removes header from map file and writes columns (prot\_id, phylostrata, ti, psname) to tmp file and imports that file into ClickHouse (needs ClickHouse connection parameters to connect to ClickHouse).
    It can use PS, TI and PSNAME config sections.

- import\_blastdb\_stats

        # options from command line
        FindOrigin.pm --mode=import_blastdb_stats -d jura --stats=t/data/analyze_hs_9606_all_ff_for_db -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_blastdb_stats -d jura --stats=t/data/analyze_hs_9606_all_ff_for_db -v

    Imports analyze stats file created by AnalyzePhyloDb.
     AnalysePhyloDb -n nr\_raw/nodes.dmp.fmt.new.sync -t 9606 -d ./all\_ff\_for\_db/ > analyze\_hs\_9606\_all\_ff\_for\_db

    It can use PS and TI config sections.

- import\_names

        # options from command line
        FindOrigin.pm --mode=import_names -d jura --names=t/data/names.dmp.fmt.new.gz -ho localhost -po 8123 -v

        # options from config
        FindOrigin.pm --mode=import_names -d jura --names=t/data/names.dmp.fmt.new.gz -v

    Imports names file (columns ti, species\_name) into ClickHouse.

- blastout\_uniq

        # options from command line
        FindOrigin.pm --mode=blastout_uniq -d jura --blastout_tbl=hs_1mil -v

        # options from config
        FindOrigin.pm --mode=blastout_uniq -d jura --blastout_tbl=hs_1mil -v

    It creates a unique non-redundant blastout\_uniq table with only relevant information (prot\_id, ti) for stratification and other purposes. Other columns (score, pgi blast hit) could be added later too.
    From that blastout\_uniq\_tbl it creates report\_gene\_hit\_per\_species\_tbl2 which holds summary of per phylostrata per species of BLAST output analysis (ps, ti, species\_name, gene\_hits\_per\_species, genelist).

- bl\_uniq\_expanded

        # options from command line
        FindOrigin.pm --mode=bl_uniq_expanded -d jura --report_ps_tbl=hs_1mil_report_per_species -v -v

        # options from config
        FindOrigin.pm --mode=bl_uniq_expanded -d jura --report_ps_tbl=hs_1mil_report_per_species -v -v

    Update report\_ps\_tbl table with unique and intersect hits and gene lists.

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

# LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Martin Sebastijan Šestak
mocnii
<msestak@irb.hr>
