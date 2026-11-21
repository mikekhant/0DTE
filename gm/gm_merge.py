import json,re,sys
src,run,out=sys.argv[1:4];new='--new' in sys.argv
h=open(src).read();R=json.load(open(run))
pat=re.compile(r'(<script id="gm-data" type="application/json">)(.*?)(</script>)',re.S)
D={} if new else json.loads(pat.search(h).group(2))
snaps=[] if new else D.get('snapshots',[])
snap=R.pop('snapshot',None)
D.update(R)
if snap:
    snaps=[s for s in snaps if s['t']!=snap['t']]+[snap]
D['snapshots']=snaps
blob=json.dumps(D,separators=(',',':')).replace('</','<\\/')
h=pat.sub(lambda m:m.group(1)+blob+m.group(3),h,count=1)
h=re.sub(r'<title>0DTE Gamma Map[^<]*</title>','<title>0DTE Gamma Map '+D['dateLabel']+'</title>',h,count=1)
open(out,'w').write(h)
