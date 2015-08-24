# Copyright (C) 2014 David Helkowski
# License CC-BY-SA ( http://creativecommons.org/licenses/by-sa/4.0/ )

package Pg::Helper;
use strict;
use warnings;
use DBI;
use Data::Dumper;

my $structure = 0;
my $by_to = {};
my $by_to_table = {};
my $by_from = {};

sub new {
    my $class = shift;
    my $conf = shift;
    my $self = {};
    
    my $host = $conf->{'host'};
    my $user = $conf->{'user'};
    my $pass = $conf->{'password'};
    
    my $dbh = DBI->connect("dbi:Pg:dbname=bfms;host=$host","$user","$pass", { AutoCommit => 0, Warn => 0 } );
    $self->{'dbh'} = $dbh;
    
    return bless $self, $class;
}

# See foreign_key_view.txt for the definition of the view "foreign_key_list"
sub get_structure {
    my $self = shift;
    return if( $structure );
    my $dbh = $self->{'dbh'};
    $structure = $dbh->selectall_arrayref("select master_table_name,master_key,foreign_table_name,foreign_key from foreign_key_list;");
    for my $row ( @$structure ) {
        my ( $from, $from_col, $to, $to_col ) = @$row;
        my $rel = {
            from     => $from,
            from_col => $from_col,
            to       => $to,
            to_col   => $to_col
        };
        $by_to->{ "$to.$to_col" } = $rel;
        $by_from->{ "$from.$from_col" } = $rel;
        $by_to_table->{ $to } ||= [];
        my $trefs = $by_to_table->{ $to };
        push( @$trefs, $rel );
    }
}

sub conds_to_where {
    my ( $self, $conds ) = @_;
    my @keys;
    my @vals;
    for my $key ( keys %$conds ) {
        my $val = $conds->{ $key };
        my $ref = ref( $val );
        if( $ref ) {
            if( $ref eq 'ARRAY' ) {
                my $cnt = scalar @$val;
                my $qs = [];
                for( my $i=0;$i<$cnt;$i++ ) {
                    push( @$qs, '?' );
                }
                push( @keys, "$key in (".join(',',@$qs).")" );
                push( @vals, @$val );
            }
            if( $ref eq 'HASH' ) {
                my $special = $val->{'special'};
                if( $special eq 'null' ) {
                    push( @keys, "$key is null" );
                }
                elsif( $special eq 'notnull' ) {
                    push( @keys, "$key is not null" );
                }
            }
        }
        else {
            push( @keys, "$key=?" );
            push( @vals, $val );
        }
        
    }
    my $keytext = join( ' and ', @keys );
    return ( $keytext, \@vals );
}

#<db_delete table="t1">
#        <join table="t2" on="t3_id" as="t3_table" />
#        <where blah="blah2" t3_table.blah4="blah5" />
#    </db_delete>
sub delete_cascade {
    my $self = shift;
    my %ops = ( @_ );
    
    my $table = $ops{'table'};
    my %conds = ( %{ $ops{'where'} } );
    
    my ( $where_text, $vals ) = $self->conds_to_where( \%conds );
    
    $self->get_structure();
    
    my $log = [ "deleting from $table where $where_text -- " . join(",", @$vals ) ];
    
    my $dbh = $self->{'dbh'};
    
    my $sth;
    if( $ops{'join'} ) {
        my $joins = $ops{'join'};
        my $joint = '';
        for my $join ( @$joins ) {
            my $jtable = $join->{'table'};
            my $on = $join->{'on'};
            my $as = $join->{'as'};
            $joint .= "join $jtable $as on $table.$on=$as.id ";
        }
        my $q = "select $table.id as id from $table $joint where $where_text";
        my $rows = $dbh->selectall_arrayref( $q, undef, @$vals );
        my @ids;
        if( $rows ) {
            for my $row ( @$rows ) {
                my $id = $row->[0];
                push( @ids, $id );
            }
        }
        if( !@ids ) {
            push( @$log, "no matching items found" );
            return $log;
        }
        
        $dbh->do('begin');
        
        my ( $w2, $v2 ) = $self->conds_to_where( { id => \@ids } );
        my $q2 = "delete from $table where $w2";
        $sth =  $dbh->prepare( $q2 );
        $sth->execute( @$v2 );
    }
    else {
        # delete from the table
        
        $dbh->do( "begin" );
        
        my $q = "delete from $table where $where_text";
        #my $numrows = $dbh->do( $q, undef, @$vals );
        $sth = $dbh->prepare( $q );
        $sth->execute( @$vals);
    }
    
    my $numrows = $sth->rows;
    
    if( $numrows ) { 
        #print Dumper( $numrows );
        #exit;
        push( @$log, "deleted $numrows, looking for references" );
        $self->remove_foreign_referring_rows( $log, "", $table );
    }
    $dbh->do( "commit" );
    return $log;
}

sub check_exists {
    my $self = shift;
    my $table = shift;
    my %conds = ( @_ );
    my ( $where_text, $vals ) = $self->conds_to_where( \%conds );
    
    my $dbh = $self->{'dbh'};
    print "Select where $where_text -- " . join( ',', @$vals ) . "\n";
    my $row = $dbh->selectrow_arrayref( "select count(*) from $table where $where_text", undef, @$vals );
    my $cnt = $row->[0];
    return $cnt;
}

#my $data = $psql->query( $test->{'table'}, $fetcharr, $where, limit => 1 );
sub query {
    my $self  = shift;
    my $table = shift;
    my $cols  = shift;
    my $conds = shift;
    my $ops   = { @_ };
    
    my ( $where_text, $vals ) = $self->conds_to_where( $conds );
    
    my $coltext = join( ',', @$cols );
    
    my $dbh = $self->{'dbh'};
    
    if( $where_text ) { $where_text = "where $where_text"; }
    
    my $q;
    if( $ops->{'query'} ) {
      $q = $ops->{'query'};
      $q =~ s/\$where/$where_text/;
    }
    else {
      my $joint = '';
      if( $ops->{'join'} ) {
        my $joins = $ops->{'join'};
        for my $join ( @$joins ) {
            my $jtable = $join->{'table'};
            my $on = $join->{'on'};
            my $as = $join->{'as'};
            $joint .= "join $jtable $as on $table.$on=$as.id ";
        }
      }
      $q = "select $coltext from $table $joint $where_text";
    }
    print STDERR "$q -- " . join(',', @$vals ) . "\n";
    
    my @colnames;
    for my $col ( @$cols ) {
      if( $col =~ m/ as (.+)$/ ) {
        push( @colnames, $1 );
      }
      else {
        push( @colnames, $col );
      }
    }
    
    if( $ops->{'limit'} && $ops->{'limit'} == 1 ) {
        my $row = $dbh->selectrow_arrayref( "$q limit 1", undef, @$vals );
        if( !$row ) { return 0; }
        my $res = db_array_result_to_hash( $row, \@colnames );
        #print Dumper( $res );
        return $res;
    }
    else { # we need to return an array ref of rows
        my $rows = $dbh->selectall_arrayref( $q, undef, @$vals );
        if( !$rows ) { return 0; }
        my @res;
        for my $row ( @$rows ) {
            push( @res, db_array_result_to_hash( $row, \@colnames ) );
        }
        return \@res;
    }
}

sub db_array_result_to_hash {
    my ( $rows, $cols ) = @_;
    my $len = scalar @$rows;
    my %hash;
    for( my $i=0;$i<$len;$i++ ) {
        my $name = $cols->[ $i ];
        my $val  = $rows->[ $i ];
        $hash{ $name } = $val;
    }
    return \%hash;
}

sub remove_foreign_referring_rows {
    my ( $self, $log, $pad, $table ) = @_;
    my $refs = $self->get_refs_to_table( $table );
    for my $ref ( @$refs ) {
        my $referencing_table = $ref->{'from'};
        my $referencing_col   = $ref->{'from_col'};
        push( @$log, "" );
        push( @$log, "${pad}referenced by $ref->{'from'}.$ref->{'from_col'}" );
        my $delcnt = $self->wipe_rows_with_null_refs( $log, $pad, $referencing_table, $referencing_col );
        if( !$delcnt ) {
            pop @$log; pop @$log;
        }
    }
}

# this deletes all null refs for a specific foreign key in a table -- returns number of rows deleted
sub wipe_rows_with_null_refs {
    my ( $self, $log, $pad, $table, $column ) = @_;
    my $dest_rel = $by_from->{ "$table.$column" };
    my $dest     = $dest_rel->{'to'};
    my $dest_col = $dest_rel->{'to_col'};
    $pad = "  $pad";
    #push( @$log, "${pad}deleting from $table where $column is a bad reference" );
    my $dbh = $self->{'dbh'};
    
    #my $subtbs = ( $table eq $dest ) ? $table : "$table,$dest";
    my $subtbs = $dest;
    my $q = "delete from $table where $table.$column is not null and not exists ( select NULL from $subtbs where $table.$column=$dest.$dest_col );";
    #push( @$log, "$pad$q" );
    my $sth = $dbh->prepare( $q );
    $sth->execute();
    my $numrows = $sth->rows;
    
    if( $numrows ) {
        #print Dumper( $sth->rows );
        #exit;
        push( @$log, "${pad}deleting from $table where $column is a bad reference" );
        push( @$log, "${pad}deleted $numrows, looking for references" );
        $self->remove_foreign_referring_rows( $log, $pad, $table );
        return $numrows;
    }
    else {
        return 0;
    }
}

sub get_refs_to_table {
    my ( $self, $table ) = @_;
    return $by_to_table->{ $table };
}

1;