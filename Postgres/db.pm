#!/usr/bin/perl

package Postgres::db;
use strict;
use warnings;
use DBI;

sub new {
    my $this = shift;
    my $class = ref($this) || $this;
    my ($user,$pass,$debug, $dbname, $auto_commit) = @{{@_}}{qw/user pass debug dbname auto_commit/};
    my $self = {};
    
    bless($self,$class);
    $self->{'user'}    = $user;
    $self->{'pass'}    = $pass;
    $self->{'dbname'} = $dbname;
    $auto_commit ||= 0;
    
    $self->{'dbh'} = DBI->connect("dbi:Pg:dbname=$dbname",$user,$pass,{AutoCommit=> $auto_commit, RaiseError=>1});
    $self->do("set constraints all deferred");
    
    $debug = 0 unless defined $debug;
    $self->{'debug'} = $debug;
    $self->{'sth'} = undef;
    #$self->{'debug'} = 1;
    return $self;    
}

sub dbh 
{
    my $self = shift;
    $self->{'dbh'};
}
sub commit 
{
    my $self = shift;
    $self->{'dbh'}->commit();
    $self->do("set constraints all deferred");
    $self->{'debug'} and warn ref($self) . "::ROLLBALCK):\n";
}
sub rollback 
{
    my $self = shift;
    $self->{'dbh'}->rollback();
    $self->do("set constraints all deferred");
    $self->{'debug'} and warn ref($self) . "::ROLLBALCK):\n";
}
sub disconnect 
{
    my $self = shift;
    $self->{'dbh'}->disconnect();
    $self->{'debug'} and warn ref($self) . "::DISCONNECT():\n";
}

sub DESTROY 
{
    my $self = shift;
	
	while ( my ($key,$val) = each(%{$self->{'sth'}}) )
	{
	    $self->{'dbh'} or last;	
		$self->{'sth'}->{$key} and $self->{'sth'}->{$key}->finish and $self->{'sth'}->{$key} = undef;
	}

    $self->{'dbh'} and $self->{'dbh'}->commit;
    $self->{'dbh'} and $self->{'dbh'}->disconnect;
    $self->{'dbh'} = undef;
    $self->{'debug'} and warn ref($self) . "::DESTROY():\n";
}

sub query_bind
{
    my $self = shift;
	my $name = shift;
    my $sql = shift;

    $self->{'debug'} and warn ref($self) . "::queryBind(): name: $name, sql: $sql : ARGS: " . join(',',@_) . "\n";
   
	!$self->{'dbh'} || !$name and return undef; 
    
    my %return;

	$self->{'sth'}->{$name} ||= $self->{'dbh'}->prepare($sql);

	$self->{'sth'}->{$name}->execute(@_);	

	$self->{'sth'}->{$name}->bind_columns( \( @return{ @{$self->{'sth'}->{$name}->{NAME_lc} } } ));

    return \%return;
}

sub fetch
{
	my $self = shift;
	my $name = shift;
	
	!$self->{'sth'}->{$name} and return undef;
	
	my $val = $self->{'sth'}->{$name}->fetch;

	!$val and $self->{'sth'}->{$name}->finish and $self->{'sth'}->{$name} = undef;

	return $val;
}

sub query
{
    my $self = shift;
    my $sql = shift;
    $self->{'debug'} and warn ref($self) . "::query(): sql: $sql : ARGS: " . join(',',@_) . "\n";
    
	!$self->{'dbh'} and return undef; 
    
	my @return;

    $self->{'sth'}->{$sql} ||= $self->{'dbh'}->prepare($sql);

    $self->{'sth'}->{$sql}->execute(@_);

	my $results = $self->{'sth'}->{$sql}->fetchall_arrayref;
 
	foreach (@{$results})
	{ 
		my @row = @{$_};
		if ( $#row < 1 )
		{
			push @return, $row[0];
		}
		else
		{
			push(@return,[@row]);
		} 
	}
 
    return (@return);
}

sub query_all_arrayref
{
    my $self = shift;
    my $name = shift;
    my $sql = shift;
    $self->{'debug'} and warn ref($self) . "::fetch_all_array(): sql: $sql : ARGS: " . join(',',@_) . "\n";
    
    !$self->{'dbh'} and return undef; 
    
    $self->{'sth'}->{$name} ||= $self->{'dbh'}->prepare($sql);

    $self->{'sth'}->{$name}->execute(@_);

    return $self->{'sth'}->{$name}->fetchall_arrayref;
}

sub do 
{
    my $self = shift;
    my $sql = shift;

    $self->{'debug'} and warn ref($self) . "::do(): sql: $sql : ARGS: " . join(',',@_) . "\n";
    if ( not $self->{'dbh'}) {
        return(undef);
    }
    $self->{'sth'}->{$sql} ||= $self->{'dbh'}->prepare($sql);
    
    $self->{'sth'}->{$sql}->execute(@_);
}

sub selectall_hashref {
    my ($self, $query, $key_field, @bind_vals) = @_;
    $self->{'debug'} and warn ref($self) . "::selectall_hashref(): sql: $query : ARGS: " . join(',',@_) . "\n";
    if ( not $self->{'dbh'}) {
        return(undef);
    }
    return $self->{'dbh'}->selectall_hashref($query, $key_field, {'RaiseError' => 1}, @bind_vals);
}
1;
