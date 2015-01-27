all:
	cob64 -zvP C91321R.cob -L$(OPENFRAME_HOME)/lib -ltdcpfh
	cob64 -zvP C91321W.cob -L$(OPENFRAME_HOME)/lib -ltdcpfh
	cp *.so $(OPENFRAME_HOME)/volume_DEFVOL/SYS1.COBLIB
	cp DCPJCL $(OPENFRAME_HOME)/volume_DEFVOL/SYS1.JCLLIB
