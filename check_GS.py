#!/usr/bin/env python

import sys, math, getopt
import time
from collections import defaultdict
import pprint
import copy
import json
import csv


sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import restful
mcm=restful(dev=False)


def get_rootID(prepid):
    root_id = prepid 
    crs = mcm.getA('chained_requests', query='contains=%s'%(prepid))
    for cr in crs:
        root_id = cr['chain'][0]
        break
    return root_id

chains = {'pu1':['RunIISpring15DR74Startup25ns','RunIISpring15DR74Startup25nswmLHE','RunIISpring15DR74Startup25nspLHE'], 
          'pu2':['RunIISpring15DR74Startup50ns','RunIISpring15DR74Startup50nswmLHE','RunIISpring15DR74Startup50nspLHE'], 
          'pu3':['RunIISpring15DR74Startup25nsRaw','RunIISpring15DR74Startup25nsRawwmLHE','RunIISpring15DR74Startup25nsRawpLHE'],           
          'pu4':['RunIISpring15DR74Startup50nsRaw','RunIISpring15DR74Startup50nsRawwmLHE','RunIISpring15DR74Startup50nsRawpLHE'],
          'pu5':['RunIISpring15DR74AsymptFlat10to5025nsRaw','RunIISpring15DR74AsymptFlat10to5025nsRawwmLHE','RunIISpring15DR74AsymptFlat10to5025nsRawpLHE'],
          'pu6':['RunIISpring15DR74StartupFlat10to5050nsRaw','RunIISpring15DR74StartupFlat10to5050nsRawwmLHE','RunIISpring15DR74StartupFlat10to5050nsRawpLHE']
          }
nPU = 6

def chain(pu,prepid):
    if prepid.split('-')[1] == 'RunIIWinter15GS': return chains[pu][0]
    elif prepid.split('-')[1] == 'RunIIWinter15wmLHE': return chains[pu][1]
    elif prepid.split('-')[1] == 'RunIIWinter15pLHE': return chains[pu][2]
    else:
        print 'check prepid',prepid
        sys.exit(1)


def group_prepids(ids):
    if len(ids) < 1:
        print 'empty group_prepids ? ', ids
        sys.exit(1)

    if len(ids) <= 1:
        return ids

    firstnb = -1

    res = []
    newid = ids[0]
    counter = 0
    ic = 0 
    for prepid in ids:

        nb = int(prepid.split('-')[2])

        if firstnb < 0:
            firstnb = nb
        else:
            if nb - firstnb >  counter:
                firstnb = nb 
                counter = 0 
                lastid = ids[ic-1]
                if lastid != newid: 
                    res.append(newid+"-"+lastid.split('-')[2])
                else:
                    res.append(newid)
                newid = prepid

            else: ###last one 
                if ic == len(ids) - 1:
                    lastid = ids[ic]
                    if lastid != newid:
                        res.append(newid+"-"+lastid.split('-')[2])
                    else:
                        res.append(newid)
                    

        counter += 1
        ic += 1

    if newid == ids[-1]:
        res.append(newid)

    return res


def get_evt(b):
    evt = 0 
    if b.rfind('-') >= 0:
        if b[b.rfind('-')+1:].isdigit():
            evt = int( b[b.rfind('-')+1:])
        #else:
            #print 'warning all events for this tag! ', b 
    
    return evt

def is_raw(b):
    return b.find('Raw') >=0 or b.find('RAW') >=0


def is_reco(b):
    return b.find('Reco') >=0 or b.find('RECO') >=0


def check_chains(rid):
    crs = mcm.getA('requests', query='prepid=%s'%(rid))
    vch = []
    for cr in crs:
        for hist in cr['history']:
            if hist.has_key('action') and hist['action']=='join chain' and  hist.has_key('step') and hist['step'].find('RunIISpring15DR74') >= 0: 
                vch.append(hist['step'].split('_')[-1].split('-')[0])
    
    return vch


def check_chained_request(prepid):
    crs = mcm.getA('chained_requests', query='contains=%s'%(prepid))

    for cr in crs:
        if cr['prepid'].find('RunIISpring15DR74') >= 0:
            if cr['chain'][-1].find('RunIISpring15DR74') < 0:
                return False

    return True
        

def main(args):
    if len(args) < 1:
        print "example usage: ./check_GS.py updateinput (or RunIIWinter15GS)"
        sys.exit(1)

    campaign = 'RunIIWinter15GS'    

    updateinput = False
    if args[0] == 'updateinput':
        print 'will update inputs/chains_RunIISpring15DR74.txt and inputs/RunIIWinterGS15_rootID.txt'
        updateinput = True

    elif args[0] != 'RunIIWinter15GS':
        print './check_GS.py updateinput (or RunIIWinter15GS)'
        sys.exit(1)

    print 'campaign', campaign


    t1 = time.strftime("%Y-%m-%d %H:%M")
    t1a = t1.replace(' ','-')

    f1 = open('inputs/chains_RunIISpring15DR74.txt','a+')
    
    rrch = f1.readlines()


    allchains_done ={}

    for rch in rrch: 
        prepid = rch.split(' ')[0]
        allchains_done[prepid] = []
        vc = rch.split(' ')
        nch = {}
        for c in vc: 
            for i in range(0,nPU):
                pu = 'pu%d'%(i+1)
                c1 = chains[pu][0]
                if c.find(c1) >= 0 : 
                    if nch.has_key(pu) == False:
                        nch[pu] = 1
                    else:
                        nch[pu] += 1
                    
        for i in range(0,nPU):    
            pu = 'pu%d'%(i+1)
            if nch.has_key(pu):
                allchains_done[prepid].append(nch[pu])
            else:
                allchains_done[prepid].append(0)


    print 'loaded all chains_done '            

    grps = []    
    
    crs = mcm.getA('requests', query='member_of_campaign=%s'%(campaign))

    allrequests = {}
    repetition = {}
    root_allgs = {}

    complete_25ns = 0    
    complete_50ns = 0    
    done_25ns = 0    
    done_50ns = 0    


    ##Load rootid files
    f2 = open('inputs/RunIIWinterGS15_rootID.txt','a+')
    rr = f2.readlines()

    rootids = {}
    for r in rr:
        rootids[r.split(' ')[0]] = r.split(' ')[1].strip()
    

    ffwarning = open('warning.txt',"w+") 
    ff_forceflow = open('need_force_flow.txt',"w+")


    for cr in crs:
        prepid = cr['prepid']
        stat = cr['status']
        evt_tot = cr['total_events']
        evt_complete = cr['completed_events']
        tag = cr['tags']


        if rootids.has_key(prepid):
            rootid = rootids[prepid]
        else:    
            rootid =  get_rootID(prepid)           
            text = prepid + ' '+ rootid+'\n'
            f2.write(text)

        if repetition.has_key(rootid) == False:
            repetition[rootid]  = 0 
        else:
            repetition[rootid]  += 1

        
        Is25ns = True
        Is25nsRaw = False
        Is25nsdrHighPrio = False

        Is50ns = False
        Is50nsRaw = False

        evt25ns = 0 
        evt50ns = 0 

        IsAsymptFlat10to5025nsRaw = False
        evtAsymptFlat10to5025nsRaw = 0
        
        IsStartupFlat10to5050nsRaw = False
        evtStartupFlat10to5050nsRaw = 0


        for b in tag:
            if b.find('50nsdr') >=0 :
                Is50ns = True
                evt50ns = get_evt(b)
                if is_raw(b): 
                    Is50nsRaw = True
                    Is50ns = False
                if is_reco(b):
                    print 'is_reco_not_implemented!!',tag,prepid
                    sys.exit(1)
                    
                if is_raw(b) and is_reco(b):
                    print 'is_raw_and_reco_not_implemented!!',tag,prepid
                    sys.exit(1)
        
            if b.find('no25ns') >=0:
                Is25ns = False

            if b.find('25nsdr') >=0:
                evt25ns = get_evt(b)
                Is25nsdrHighPrio =  b.find('HighPrio')>=0
                evt25ns = get_evt(b)
                if is_raw(b):
                    Is25nsRaw = True
                    Is25ns = False
                if is_reco(b):
                    print 'is_reco_not_implemented!!',tag,prepid
                    sys.exit(1)
                
                if is_raw(b) and is_reco(b):
                    print 'is_raw_and_reco_not_implemented!!',tag,prepid
                    sys.exit(1)

            if b.find('AsymptFlat10to5025nsRaw')  >=0:
                IsAsymptFlat10to5025nsRaw = True
                evtAsymptFlat10to5025nsRaw = get_evt(b)

            if b.find('StartupFlat10to5050nsRaw')  >=0:
                IsStartupFlat10to5050nsRaw = True
                evtStartupFlat10to5050nsRaw = get_evt(b)
                
                
        if Is25nsRaw and Is25ns: 
            print 'waring both Is25nsRaw and Is25ns ?', prepid
            sys.exit(1)
            

        if stat != 'done': continue
                
        if evt25ns > evt_complete or evt50ns > evt_complete :
            print 'evt_asked > evt_complete ? ', evt25ns, evt50ns, evt_complete, prepid
            sys.exit(1)

        if evt_complete < 0.95 * evt_tot:
            if check_chained_request(prepid) == False:
                text = "evt_complete/done=%d/%d\n"%(evt_complete,evt_tot)
                ff_forceflow.write(text)
                text = 'https://cms-pdmv.cern.ch/mcm/chained_requests?contains=%s&page=0&shown=15\n'%prepid
                ff_forceflow.write(text)
    

        
        vvc = []
        vvc.append(Is25ns)
        vvc.append(Is50ns)
        vvc.append(Is25nsRaw)
        vvc.append(Is50nsRaw)
        vvc.append(IsAsymptFlat10to5025nsRaw)
        vvc.append(IsStartupFlat10to5050nsRaw)


        chaindone = True
        if allchains_done.has_key(prepid): 
            kk = 0 
            for nc in allchains_done[prepid]:
                if vvc[kk] and nc == 0 :
                    chaindone = False
                kk += 1    
        else:
            chaindone = False

        
        if repetition[rootid]==0 and chaindone: 
            continue


        ##write to text to update the inputs/chains_*.txt
        if allchains_done.has_key(prepid) == False or chaindone == False:
            rid = rootids[prepid]
            vch = check_chains(rid)
            text = prepid + ' '+ rid + ' '+ str(tag) + ' '+ str(vch) + '\n'
            f1.write(text)

        
        if root_allgs.has_key(rootid) == False:
            root_allgs[rootid] = []

        root_allgs[rootid].append(prepid)
    

        if allrequests.has_key(rootid) == False:   ##only one rootID (last one) saved into allrequests!!!
            allrequests[rootid] = []
            if Is25ns: 
                allrequests[rootid].append(evt25ns) ##evt
                if Is25nsdrHighPrio: allrequests[rootid].append(3) ##block
                else: allrequests[rootid].append(4) ##block

            else:
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)

            if Is50ns:
                allrequests[rootid].append(evt50ns) ##evt                                                                                                                                      
                allrequests[rootid].append(3) ##block 
            else:
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)

            if Is25nsRaw:
                allrequests[rootid].append(evt25ns) ##evt 
                if Is25nsdrHighPrio: allrequests[rootid].append(3) ##block                                                                                                                                  
                else: allrequests[rootid].append(4) ##block 
            else: 
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)
                
            if Is50nsRaw:
                allrequests[rootid].append(evt50ns) ##evt                                                                                                                                      
                allrequests[rootid].append(3)
            else:
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)
   
            if IsAsymptFlat10to5025nsRaw:
                allrequests[rootid].append(evtAsymptFlat10to5025nsRaw)
                allrequests[rootid].append(3)
            else:
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)

            if IsStartupFlat10to5050nsRaw:
                allrequests[rootid].append(evtStartupFlat10to5050nsRaw)
                allrequests[rootid].append(3)
            else:
                allrequests[rootid].append(-1)
                allrequests[rootid].append(0)


    if updateinput:
        print 'inputs/*txt up to date'
        print 'now run ./check_GS.py RunIIWinter15GS'
        return

                

    b = sorted(allrequests)
    y = []
    yy = []
    lastid = None
    for key in b:
        value = allrequests[key]
        if not lastid:
            lastid = key
            y.append(key)
        if key.split('-')[0] == lastid.split('-')[0] and key.split('-')[1] == lastid.split('-')[1]:
            if key != lastid: y.append(key)
        else:
            lastid = key
            yy.append(y)
            y = []
            y.append(key)

    if len(y) >= 1: 
        yy.append(y)

             
    ffo = open('alltickets.txt', "w+")

    for y in yy: 
        ##loop over all id in y and for each pu
        for i in range(0,nPU):
            pu = 'pu%d'%(i+1)

            l1 = [] ##block2
            l2 = [] ##block3
            l3 = [] ##block4

            blk = 0 
            evt = 0 
            for rid in y:

                if repetition[rid] > 0:
                    ffwarning.write('warning repetition!!'+rid+' '+str(repetition[rid])+' '+str(root_allgs[rid])+'\n')
                    continue

                idgs = root_allgs[rid][0]
                if allchains_done.has_key(idgs) and allchains_done[idgs][i] >= 1: 
                    continue
                
                if allrequests[rid][i*2] == 0 :  ##evt
                    if allrequests[rid][i*2+1]==2: l1.append(rid)
                    elif allrequests[rid][i*2+1]==3:  l2.append(rid)
                    elif allrequests[rid][i*2+1]==4:  l3.append(rid)
                    else:
                        print 'block_0_allstat?? ',allrequests[rid][i*2+1],idgs
                        sys.exit(1)


            #print pu
            if len(l1) >= 1: 
                t =  chain(pu,l1[0])+ ' 2 0'
                ffo.write(t)
                for ii in group_prepids(l1): 
                    ffo.write(' ')
                    ffo.write(ii)
                ffo.write('\n')    
            if len(l2) >= 1: 
                t =  chain(pu,l2[0])+ ' 3 0'
                ffo.write(t)
                for ii in group_prepids(l2):
                    ffo.write(' ')
                    ffo.write(ii)
                ffo.write('\n')

            if len(l3) >= 1:
                t =  chain(pu,l3[0])+ ' 4 0'
                ffo.write(t)
                for ii in group_prepids(l3):
                    ffo.write(' ')
                    ffo.write(ii)
                ffo.write('\n')

            ##for partial statistics
            pevt = []            
            for rid in y:
                if repetition[rid] > 0:
                    continue

                idgs = root_allgs[rid][0]
                if allchains_done.has_key(idgs) and allchains_done[idgs][i] >= 1:
                    continue

                if allrequests[rid][i*2] > 0: 
                    if pevt.count(allrequests[rid][i*2]) == 0:
                        pevt.append(allrequests[rid][i*2])
            
            for evt in pevt:
                pl1 = []
                pl2 = []
                pl3 = []
                for rid in y:
                    if repetition[rid] > 0:
                        continue

                    if allrequests[rid][i*2] == evt :  ##partial evt
                        if allrequests[rid][i*2+1]==2: pl1.append(rid)
                        elif allrequests[rid][i*2+1]==3:  pl2.append(rid)
                        elif allrequests[rid][i*2+1]==4:  pl3.append(rid)
                        else:
                            print 'block_0_partstat?? ',allrequests[rid][i*2+1],idgs
                            sys.exit(1)

                if len(pl1) >= 1:
                    t =  chain(pu,pl1[0])+ ' 2 '+str(evt)
                    ffo.write(t)
                    for ii in group_prepids(pl1):
                        ffo.write(' ')
                        ffo.write(ii)
                    ffo.write('\n')

                if len(pl2) >= 1:
                    t =  chain(pu,pl2[0])+ ' 3 '+str(evt)
                    ffo.write(t)
                    for ii in group_prepids(pl2):
                        ffo.write(' ')
                        ffo.write(ii)
                    ffo.write('\n')

                if len(l3) >= 1:
                    t =  chain(pu,pl3[0])+ ' 4 '+str(evt)
                    ffo.write(t)
                    for ii in group_prepids(pl3):
                        ffo.write(' ')
                        ffo.write(ii)
                    ffo.write('\n')
    

    ffo.close()

                
if __name__ == '__main__':
    main(sys.argv[1:])


