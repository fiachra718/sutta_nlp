#!	/usr/bin/perl -w
use strict;

open(TSV, "./graph/entities/reports/unresoled_person.tsv") or die $!;
my @lines;
my $search_person;

while(<TSV>){
	@lines = split(/\t/,$_);
	#  These are lines up
	# as tag \t name \t count
	# do get item at index 1
	$search_person = $lines[1];
	my $res = do {
	    local $/;
	    open my $fh, "-|",
	        "python", "ne-data/scripts/training_spans.py",
	        "--search-term", $search_person
	        or die "open failed: $!";
	    <$fh>;
	};
	print($res);
	print ("-----\n\n");

	# $res = `python term_search_db.py $search_person`;
	# fails for ', " in string

	$res = do {
	    local $/;
	    open my $fh, "-|",
	        "python", "term_search_db.py",  # argv[0]
	        $search_person  # argv[1]
	        or die "open failed: $!";
	    <$fh>;
	};
	print($res);
	print ("--\n\n");
}