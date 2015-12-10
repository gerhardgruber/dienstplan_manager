#!/usr/bin/perl

use strict;
use CGI;
use POSIX;
use Date::Calc qw( :all );
use DBI;
use DBD::mysql;
use DMLib;
use JSON;
use utf8;
use File::Basename;

my @people = (
  "Gabi",
  "Verena",
  "Sabrina",
  "Ingrid",
  "Angelika",
  "Toni",
  "Daniel",
  "Kathrin",
  "Claudia",
  "Jaqui",
  "Nadine",
  "Anita H.",
  "Anita N.",
  "Julia",
  "Lisa",
  "Ana",
  "Jasmin",
  "Silke"
);

  my $width = 100 / ( $#people + 2 );
  $width *= 10;
  $width = floor( $width );
  $width /= 10;

my $q = CGI->new;

mkLog( "D", 0, "===========================================" );
mkLog( "D", 0, "New request" );
mkLog( "D", 0, "===========================================" );

# Read config...
my $conf = read_config( );

my $dbh = DBConnect( "DBI:mysql:database=$conf->{ 'db' }->{ 'db' };host=$conf->{ 'db' }->{ 'host' }", $conf->{ 'db' }->{ 'user' }, $conf->{ 'db' }->{ 'pw' } );

my $func = $q->param( "func" ) || "";
mkLog( "D", 0, "func: $func" );

if ( $func eq "saveval" )
{
  saveval( 1, 2016 );
}
else
{
  print $q->header( "text/html" );

  my $data;
  $data->{ "dp_table" } = table_head( ) . table_rows( 1, 2016 );

  warn "render!";
  render( "dienstplan_manager.html", $data );
}

$dbh->disconnect( );

exit( 0 );


sub render
{
  my $file = "/srv/www/htdocs/dienstplan_manager/" . shift;
  my $data = shift;

  unless( open( FIN, "<$file" ) )
  {
    warn "Could not open $file for reading! $!";
    exit( 1 );
  }
  binmode FIN, ":encoding(utf8)";
  my $content;
  {
    local $/;
    $content = <FIN>;
  }
  close FIN;

  foreach my $k ( keys %$data )
  {
    $content =~ s/\$$k/$data->{ $k }/ig;
  }

  print $content;
}


sub table_head
{
  my $head = "<thead><tr class='table-head heading'>";
  $head .= "<th class='table-head-cell' style='width: $width%;'></th>";
  foreach my $people ( @people )
  {
    $head .= "<th class='table-head-cell' style='width: $width%;'>$people</th>";
  }
  $head .= "</tr></thead>";
  return $head;
}

sub table_rows
{
  my $month = shift;
  my $year = shift;

  my $days = Days_in_Month( $year, $month );

  my $rows = "<tbody>";
  for ( my $day = 1; $day <= $days; $day ++ )
  {
    my ( $dtxt ) = split( / /, Date_to_Text( $year, $month, $day, "3" ) );
    $dtxt = substr( $dtxt, 0, 2 );

    my $weekend = "";
    if ( $dtxt eq "Sa" || $dtxt eq "So" || $day == 1 || $day == 6 )
    {
      $weekend = "weekend";
    }

    $rows .= "<tr class='table-row $weekend'>\n";

    $rows .= sprintf( "<td class='table-row-cell heading $weekend'>$dtxt %02d</td>\n", $day, $month, $year );

    my $people_index = 0;
    foreach my $people ( @people )
    {
      $people_index ++;
      my $sth;
      unless( $sth = MySqlExec( $dbh, "select dienst from dienst where person = ? and day = ? and month = ? and year = ?", $people_index, $day, $month, $year ) )
      {
        mkLog( "F", 0, "DB-Error!" );
        exit( 1 );
      }
      my ( $dienst ) = $sth->fetchrow_array( );
      $sth->finish( );
      $rows .= "<td><input class='day' type='text' value='$dienst' style='width: 100%;' data-day='${day}' data-person='${people_index}' id='${day}_${people_index}'></input></td>";
    }

    $rows .= "</tr>";
  }
  $rows .= "</tbody>";
  return $rows;
}

sub saveval
{
  my $month = shift;
  my $year = shift;

  my $person = $q->param( "person" );
  my $day = $q->param( "day" );
  my $dienst = $q->param( "val" );

  my $sth;
  unless( $sth = MySqlExec( $dbh, "delete from dienst where person = ? and day = ? and month = ? and year = ?", $person, $day, $month, $year ) )
  {
    mkLog( "F", 0, "DB-Error" );
    exit( 1 );
  }
  $sth->finish( );

  unless( $sth = MySqlExec( $dbh, "insert into dienst (person, day, month, year, dienst) values(?, ?, ?, ?, ?)", $person, $day, $month, $year, $dienst ) )
  {
    mkLog( "F", 0, "DB-Error" );
    exit( 1 );
  }
  $sth->finish( );

  success( );
}

sub success
{
  print $q->header( "text/json" );
  print encode_json( {
    "success" => JSON::true
  } );
}

sub read_config
{
  my $config_file = getDir( dirname( $0 ) ) . "config.json";
  unless( open( FIN, "<$config_file" ) )
  {
    mkLog( "F", 0, "Error while opening $config_file! $!" );
    exit( 1 );
  }
  my $config_content;
  {
    local $/;
    $config_content = <FIN>;
  }
  close FIN;

  return decode_json( $config_content );
}