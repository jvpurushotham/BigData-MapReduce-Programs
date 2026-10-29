#!/usr/bin/env python3
"""Remove Kerberos/security <property> blocks from Hadoop *-site.xml files (a backup .bak is kept).
    python3 disable_security.py /usr/local/hadoop/etc/hadoop
"""
import os, re, shutil, sys

conf = sys.argv[1]
BAD = re.compile(r"kerberos|principal|keytab|block\.access\.token|data\.transfer\.protection|dfs\.http\.policy|"
                 r"container-executor|spnego|dfs\.datanode\.address|dfs\.datanode\.http\.address|dfs\.datanode\.https\.address|"
                 r"hadoop\.security|hadoop\.rpc\.protection|hadoop\.http\.authentication|dfs\.encrypt|dfs\.https|"
                 r"yarn\.nodemanager\.linux-container-executor|yarn\.http\.policy", re.I)
PROP = re.compile(r"[ \t]*<property>.*?</property>[ \t]*\n?", re.S)
NAME = re.compile(r"<name>\s*(.*?)\s*</name>", re.S)

for fn in ("core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml"):
    p = os.path.join(conf, fn)
    if not os.path.exists(p):
        continue
    text = open(p).read()
    removed = []
    def drop(m):
        n = NAME.search(m.group(0))
        if n and BAD.search(n.group(1)):
            removed.append(n.group(1)); return ""
        return m.group(0)
    new = PROP.sub(drop, text)
    if removed:
        shutil.copy(p, p + ".bak")
        open(p, "w").write(new)
    print("%-16s removed %d: %s" % (fn, len(removed), ", ".join(removed) or "-"))
