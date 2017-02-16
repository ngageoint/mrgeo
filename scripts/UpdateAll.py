#!/usr/bin/python

import getpass
import os

from UpdateWikiDocuments import *

noPassword = 0;

pwfile = os.environ["HOME"] + "/.automaton-passwd"
if os.path.exists(pwfile):
    fp = open(pwfile)
    password = fp.read().rstrip('\n')
    username = "automaton"
else:
    username = raw_input("Please enter your redmine username: ")
    password = getpass.getpass()

print "Beginning retrieval of Admin Documentation"
updateWiki(username, password, "https://107.23.31.196/redmine/projects/mrgeo/", "AdminDocumentation",
           os.environ["MRGEO_HOME"] + "/docs/AdminDocumentation.html")
print "Completed retrieval of Admin Documentation"
print "Beginning retrieval of User Documentation"
updateWiki(username, password, "https://107.23.31.196/redmine/projects/mrgeo/", "UserDocumentation",
           os.environ["MRGEO_HOME"] + "/docs/UserDocumentation.html")
print "Completed retrieval of User Documentation"
# updateWiki(username, password, "https://107.23.31.196/redmine/projects/mrgeo/", "DeveloperDocumentation", os.environ["MRGEO_HOME"] + "/docs/DeveloperDocumentation.html")
# updateWiki(username, password, "https://107.23.31.196/redmine/projects/mrgeo/", "IntelUserDocumentation", os.environ["MRGEO_HOME"] + "/docs/IntelUserDocumentation.html")
