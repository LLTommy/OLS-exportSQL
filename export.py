#!/usr/bin/python
print "Starting OLS-SQL Export process"

import json
import urllib2
import sys
import MySQLdb
import ConfigParser


if len(sys.argv)!=1:
    print "Please define all properties in the config file"

else:

    print "Connecting to SQL Database"

    configParser = ConfigParser.RawConfigParser()
    configFilePath = r'config.txt'
    configParser.read(configFilePath)

    host = configParser.get('SQL config', 'path')
    username = configParser.get('SQL config', 'username')
    password = configParser.get('SQL config', 'password')
    database = configParser.get('SQL config', 'database')

    # Open database connection
    db = MySQLdb.connect(host, username, password, database)
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # execute SQL query using execute() method.
    cursor.execute("SELECT VERSION()")
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print "Database version : %s " % data

    URL=configParser.get('Service URL', 'url')

    #URL=sys.argv[1]
    relationshiplist=[]

    try:
        data=json.load(urllib2.urlopen(URL))
        pagesize=data["page"]["totalElements"]

        tmpcounter=0;
        tmprelationshipcounter=0;
        URL=URL+"?size=%i" % pagesize

        print "Pagesize and URL"
        print pagesize
        print URL

        try:
                data=json.load(urllib2.urlopen(URL))
                print "\n Webservice called worked, print data \n\n"
                termlist=data["_embedded"]["terms"]

                for counter in termlist:
                    tmpcounter+=1
                    print "\n######################### This is run number %s #########################" % tmpcounter
                    print "---Ugly SQL Table Meta-------"
                    #meta_id       INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #meta_key      VARCHAR(64) NOT NULL,
                    #meta_value    VARCHAR(128),
                    #species_id    INT UNSIGNED DEFAULT NULL,


                    print "---Ugly SQL Table ontology-------"
                    #ontology_id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #name          VARCHAR(64) NOT NULL,
                    #namespace     VARCHAR(64) NOT NULL,
                    print "Ontology name? "+counter["ontology_name"]
                    print "What is namespace supposed to be?"

                    cursor = db.cursor()
                    # Prepare SQL query to INSERT a record into the database.
                    sql = "INSERT INTO ontology(name, namespace) VALUES ('%s', '%s')" %  (counter["ontology_name"], "namespace")

                    try:
                        # Execute the SQL command
                        cursor.execute(sql)
                    except:
                        db.rollback()



                    print "---Ugly SQL Table Term-------"
                    #term_id       INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #ontology_id   INT UNSIGNED NOT NULL,
                    #subsets       TEXT,
                    #accession     VARCHAR(64) NOT NULL,
                    #name          VARCHAR(255) NOT NULL,
                    #definition    TEXT,
                    #is_root       INT NOT NULL DEFAULT 0,
                    #is_obsolete   INT NOT NULL DEFAULT 0,

                    print "(accession?) "+counter["iri"]
                    print "(name?) "+counter["label"]
                    print "(definition?) "
                    print counter["description"]
                    print "Is Obsolete? %s " %(counter["is_obsolete"])
                    print "Is root? %s" % (counter["is_root"])

                    print "---Ugly SQL Table Synonyms-------"
                    # synonym_id    INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    # term_id       INT UNSIGNED NOT NULL,
                    # name          TEXT NOT NULL,
                    # type		ENUM('EXACT', 'BROAD', 'NARROW', 'RELATED'),
                    if (type(counter["synonyms"])!=type(None)):
                        print "synonyms name %s "% counter["synonyms"]
                    else:
                        print "synonyms: NONE present"

                    cursor = db.cursor()
                    # Prepare SQL query to INSERT a record into the database.


                    sql = "SELECT ontology_id FROM ontology WHERE name = '%s'" % (counter["ontology_name"])
                    print sql

                    try:
                        # Execute the SQL command
                        cursor.execute(sql)
                        # Fetch all the rows in a list of lists.
                        results = cursor.fetchall()
                        #print results
                        for row in results:
                            ontology_id = row[0]

                        print ontology_id

                        if counter["is_root"]=='false':
                            tmproot=1
                        else:
                            tmproot=0

                        if counter["is_obsolete"]=='false':
                            tmpobsolete=1
                        else:
                            tmpobsolete=0

                        sql = "INSERT INTO term(ontology_id, subsets, accession, name, definition, is_root, is_obsolete) VALUES ('%s', '%s', '%s', '%s', \"%s\", '%s', '%s')" %\
                          (ontology_id, "subset", counter["iri"], counter["label"], counter["description"], tmproot, tmpobsolete)

                        # Execute the SQL command
                        print sql
                        cursor.execute(sql)
                    except:
                        db.rollback()



                    print "---Ugly SQL Table Subset-------"
                    #CREATE TABLE subset (
                    #subset_id     INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #name          VARCHAR(64) NOT NULL,
                    #definition    VARCHAR(128) NOT NULL,

                    print "---Ugly SQL Table alt id-------"
                    #alt_id        INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #term_id       INT UNSIGNED NOT NULL,
                    #accession     VARCHAR(64) NOT NULL,

                    print "---Ugly SQL relationship_type-------"
                    #relation_type_id  INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #name              VARCHAR(64) NOT NULL,


                    print "---Ugly SQL relationship-------"
                    # relation_id       INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    # child_term_id     INT UNSIGNED NOT NULL,
                    # parent_term_id    INT UNSIGNED NOT NULL,
                    # relation_type_id  INT UNSIGNED NOT NULL,
                    # intersection_of   TINYINT UNSIGNED NOT NULL DEFAULT 0,
                    # ontology_id       INT UNSIGNED NOT NULL,

                    try:
                        graphdata=json.load(urllib2.urlopen(counter["_links"]["graph"]["href"]))
                                #print "\n Nodes:"
                                #print graphdata["nodes"]
                                #print "\n Edges:"
                                #print graphdata["edges"]
                        print "\nSecond Webservice Call - graph - to find relationships"

                        for edge in graphdata["edges"]:
                            tmprelationshipcounter+=1;
                            print "\n"
                            print "     PartentTerm(?Source) "+edge["source"]
                            print "     ChildTerm(?Target) "+edge["target"]
                            print "     RelationShipType "+edge["label"]
                            print "     intersection_of - No idea"
                            print "     ontology_id "+counter["ontology_name"]
                            relationshiplist.append(edge["label"])

                            cursor = db.cursor()
                            # Prepare SQL query to INSERT a record into the database.
                            sql = "INSERT INTO relation(child_term_id, parent_term_id, relation_type_id, intersection_of) VALUES ('%s', '%s')" %  (counter["ontology_name"], "namespace")

                        #    try:
                        #        # Execute the SQL command
                        #        cursor.execute(sql)
                        #    except:
                        #        db.rollback()


                    except:
                        print "Error within second webservice call"
                        raise




                    print "\n---Ugly SQL closure-------"
                    #closure_id        INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    #child_term_id     INT UNSIGNED NOT NULL,
                    #parent_term_id    INT UNSIGNED NOT NULL,
                    #subparent_term_id INT UNSIGNED,
                    #distance          TINYINT UNSIGNED NOT NULL,
                    #ontology_id       INT UNSIGNED NOT NULL,


        except:
                    print "Error in a webservice call, that might be ok, we reached the end of the page. %s" %URL
                    raise


    except:
        print "Some error occured - maybe the webservice is down?! I tried to fetch data from %s" %URL
        raise

    print "\nWork is done!\nNumber of terms processed %s, Number of edges (relationships) processed %s " %(tmpcounter, tmprelationshipcounter)

    #print "show me relationshiplist"
    #print relationshiplist


    # Commit your changes in the database
    db.commit()

    # disconnect from SQL server
    db.close()
