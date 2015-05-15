# MCM 

login to lxplus.cern.ch

under CMSSW_5_3_22/src

source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh

cmsenv

./check_GS.py updateinput

./check_GS.py RunIIWinter15GS


now look at alltickets.txt to create new mccm tickets. Each line is for a ticket. 

First column is the "chains", second is the "block", third is the "staged" (if it is not 0, then
need to put in the ticket), and the rest is the prepids 


