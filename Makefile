problemset1_submission_tianyizh.tar.gz:
problemset1_submission_tqchen.tar.gz:
	rm -rf $@
	cd problemset1/parta;make clean;cd -
	cd problemset1/partb;make clean;cd -
	tar cvzf $@ $+
