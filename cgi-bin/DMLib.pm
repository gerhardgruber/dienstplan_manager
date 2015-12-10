package main;

use Cwd;
use FileHandle;
$DP::DEBUG=0;
$DP::holdlock=0;
use HTML::Entities ();

use URI::Escape;
use MIME::Base64;
use Data::Dumper;

# use Log::Log4perl;
use Sys::Hostname;

$LOG4P::init=0;

# 20090203 :: JG  :: Meldung  bei Sig HUP korrigiert. teminating -> not terminating; da wird dasProgramm nicht beendet

# $main::version = "DMLib 3.3.1 - 03. 02. 2009";
# Rerun execute ( max. 10 times ) on SQL-Statements, when error-code is 1205 (Message: Lock wait timeout exceeded; try restarting transaction)
$main::version = "DMLib 3.3.2 - 02. 06. 2015";

### Lock fuer Explain-Funktion
$main::EXPLAIN = "0";
$DP::restartHour = -1;
$DP::restartChecks = 0;

# Return-Values of OSSYS method
my $SYS_UX = 2;
my $SYS_WIN = 1;

# -------------------------------------------------------------------------
# strip off eol and leading blanks
# parameter1: reference to scalar
#
sub trim
{
  my $fld = shift;

  if ( ref( $fld ) )
  {
    $$fld =~ s/^\s+//;
    $$fld =~ s/\s+$//;
  }
  else
  {
    $fld =~ s/^\s+//;
    $fld =~ s/\s+$//;
    return $fld;
  }
}
# -------------------------------------------------------------------------
# strip off eol
# parameter1: reference to scalar
#
sub trimr
{
  my $fld=shift;
  if ( ref( $fld ) )
  {
    $$fld =~ s/\s+$//;
  }
  else
  {
    $fld =~ s/\s+$//;
    return $fld;
  }
}
# -------------------------------------------------------------------------
# strip off leading blanks
# parameter1: reference to scalar
#
sub triml
{
  my $fld = $_[ 0 ];
  if ( ref( $fld ) )
  {
    $$fld =~ s/^\s+//;
  }
  else
  {
    $fld =~ s/^\s+//;
    return $fld;
  }
}


# -------------------------------------------------------------------------
# get datetime
# parameter 1: time()-value
#
sub getdatetime
{
  my $tval = shift;

  if ( !defined $tval )
  {
    $tval = time( );
  }

  my( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime( $tval );
  $mon++;
  my $scrf = sprintf( "%02d.%02d.%04d %02d:%02d:%02d", $mday, $mon, $year + 1900, $hour, $min, $sec );
  return $scrf;
}


# -------------------------------------------------------------------------
# get time
# parameter 1: time()-value
#
sub gettime
{
  my $tval = shift;

  if ( !defined $tval )
  {
    $tval = time( );
  }

  my( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime( $tval );
  $mon++;
  my $scrf = sprintf( "%02d%02d%02d", $hour, $min, $sec );
  return $scrf;
}

sub gettim_cd
{

	      local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
					    localtime($_[0]);
         $mon++;
         return sprintf("%04d-%02d-%02d %2d:%02d:%02d",$year+1900,$mon,$mday,$hour,$min,$sec);
}

# -------------------------------------------------------------------------
# get  date
# parameter 1: time()-value
#
sub getdate
{
   my $tval=shift;
   if (! defined $tval) {$tval = time(); }
   my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
					    localtime($tval);
   $mon++;
   my $scrf=sprintf("%04d%02d%02d",$year+1900,$mon,$mday);
   return $scrf;
}

# -------------------------------------------------------------------------
# get MYSQL DateTime
# parameter 1: time()-value
#
sub getMySqlDTime
{
   my $tval=shift;
   if (! defined ($tval) ) { $tval=time(); }
   my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
				    localtime($tval);
   $mon++;
   my $scrf=sprintf("%04d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon,$mday,$hour,$min,$sec);
   return $scrf;
}
# -------------------------------------------------------------------------
# get MYSQL time
# parameter 1: time()-value
#
sub getMySqltime
{
   my $tval=shift;
   if (! defined $tval) {$tval = time(); }
   my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
   	    localtime($tval);
   $mon++;
   my $scrf=sprintf("%02d:%02d:%02d",$hour,$min,$sec);
   return $scrf;
}
# -------------------------------------------------------------------------
# get MSQL date
# parameter 1: time()-value
#
sub getMySqldate {
   my $tval=shift;
   if (! defined $tval) {$tval = time(); }
	my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
	    localtime($tval);
   $mon++;
   my $scrf=sprintf("%04d-%02d-%02d",$year+1900,$mon,$mday);
   return $scrf;
}
# -------------------------------------------------------------------------
# MSQL date
# parameter 1: Datum
#
sub MySqldate
{
   my $tval=shift;
   if ($tval=~ /\./)
   {
      my(@wrt)=split(/\./,$tval);
      return(sprintf("%04d-%02d-%02d",$wrt[2],$wrt[1],$wrt[0]));
   }
   return $tval;
}

# =====================================================================
#    Sperre einer/mehrerer Tabelle(n)
# =====================================================================

sub MySqlTblLock {
   my $dbh=shift;
   my @TBName=@_;
   my $lkStr="LOCK TABLE";
   my $k;
   my $sth;
   $DP::holdlock=1;

   foreach $k (@TBName)
   {
      $lkStr.= " $k write,";
   }
   chop $lkStr;
   $sth=MySqlExec($dbh,$lkStr);
   $sth->finish();

}

# =====================================================================
#    Unlock von Tabellen
# =====================================================================

sub MySqlTblUlk {
   my $dbh=shift;
   $DP::holdlock=0;
   my $lkStr="UNLOCK TABLE";
   my $sth;
   $sth=MySqlExec($dbh,$lkStr);
   $sth->finish();
}


# =====================================================================
#    Aufsetzen und Freigeben von Locks
# =====================================================================
sub MySqlGetLock
{
  my ( $dbh, $p, $v, $lockn, $lockt, $try ) = get_pv_args( @_ );

  my $sth;
  my $lf    = 0;

  if ( !defined $lockt )
  {
    $lockt = 1;
  }

  while ( $lf == 0 )
  {
    $sth = MySqlExec( $dbh, $p, $v, "SELECT GET_LOCK(?,?)", $lockn, $lockt );
    ( $lf ) = $sth->fetchrow_array( );
    $sth->finish( );

    if ( $try == 1 )
    {
      return $lf;
    }
  }

  return $lf;
}

sub MySqlRelLock
{
  my ( $dbh, $p, $v, $lockn ) = get_pv_args( @_ );
  my $sth;

  $sth = MySqlExec( $dbh, $p, $v, "SELECT RELEASE_LOCK(?)", $lockn );
  $sth->finish( );
}


##
#
#
##

sub MySqlEsc {
   my $tval=shift;
#              \0   	An ASCII 0 (NUL) character.
#              \' 	A single quote (�'�) character.
#              \" 	A double quote (�"�) character.
#              \b 	A backspace character.
#              \n 	A newline (linefeed) character.
#              \r 	A carriage return character.
#              \t 	A tab character.
#              \Z 	ASCII 26 (Control-Z). See note following the table.
#              \\ 	A backslash (�\�) character.
#              \% 	A �%� character. See note following the table.
#              \_ 	A �_� character. See note following the table.

   if (ref($tval))
   {
      if (! defined $$tval) { $$tval='\\0'; return; }
      $$tval =~ s/'/\\'/g;
      $$tval =~ s/"/\\"/g;
      $$tval =~ s/\b/\\b/g;
      $$tval =~ s/\n/\\n/g;
      $$tval =~ s/\r/\\r/g;
      $$tval =~ s/\t/\\t/g;
      $$tval =~ s/\Z/\\Z/g;
      $$tval =~ s/\%/\\%/g;
      $$tval =~ s/_/\\_/g;
   }
   else
   {
      if (! defined $tval) { return '\\0'; }
      $tval =~ s/'/\\'/g;
      $tval =~ s/"/\\"/g;
      $tval =~ s/\b/\\b/g;
      $tval =~ s/\n/\\n/g;
      $tval =~ s/\r/\\r/g;
      $tval =~ s/\t/\\t/g;
      $tval =~ s/\Z/\\Z/g;
      $tval =~ s/\%/\\%/g;
      $tval =~ s/_/\\_/g;
      return $tval;
   }

}


# =====================================================================
#    execute an SQL statement with $dbh->do()
# =====================================================================

sub MySqlDo {
   my $dbh=shift;
   my $cmd=shift;
   my @para=@_;
   my $sth1;

   if (! defined($dbh))
   {
      my $etxt="[MySqlDo]: dbh not defined!\n";
      Errout('F',157050,$etxt,$cmd);
      die ("");
   }
   $sth1=$dbh->do($cmd,@para);
   unless ($sth1)
   {
     $MAIN::DBIERROR="[" . $sth1->err() . "] " . $sth1->errstr() . $cmd;
     my $etxt="[" . $dbh->err() . "] " . $dbh->errstr() . "->$cmd";
     $etxt.="\nNumber of Parameters: " . ($#para + 1) . "\n";
      my $k;
      my $pi=1;
      for $k (@para)
      {
         $etxt.=sprintf(" Param%3d: (%3d): %s\n",$pi++,length($k),$k);
      }
      Errout('F',157051,$etxt,$cmd);
      return;
   }

   return $sth1;
}
# =====================================================================
#    prepare cached and execute a SQL statement
# =====================================================================

sub MySqlExec
{
  my ( $dbh, $p, $v, $cmd, @para ) = get_pv_args( @_ );

  my $sth1;
  $DBI::ERR    = 0;
  $DBI::ERRTXT = "";

  if ( ! defined( $cmd ) )
  {
    my $etxt = "[MySqlExec]: Sql Command undefined!\n";
    $$p{ "errortext" } = $etxt;
    $$p{ "form" } = $EForm::DOCPIPE;
    Errout( 'F', 157052, $etxt, $cmd );
    dumpStack( );
    return;
  }
  if ( ! defined( $dbh ) )
  {
    my $etxt = "[MySqlExec]: dbh not defined!\n";
    $$p{ "errortext" } = $etxt;
    $$p{ "form" } = $EForm::DOCPIPE;
    Errout( 'F', 157053, $etxt, $cmd );
    dumpStack( );
    return;
  }

  if ( $main::DEBUG || $DP::DEBUG )
  {
    mkLog( 'E', 403090, "Statement: $cmd Parameters: \" @para \"" );
  }
  mkSql( $dbh, $p, $v, $cmd, @para );

  my $trys = 0;
  my $enr = 0;

  do
  {
    $trys ++;
    unless ( $sth1 = MySqlPrep( $dbh, $p, $v, $cmd ) )
    {
      return;
    }
    $sth1->execute( @para );
    $enr = 0;
    if ( $sth1->err( ) )
    {
      $DBI::ERR = $enr = $sth1->err( );
      $DBI::ERRTXT = $MAIN::DBIERROR = "[ $enr ] " . $sth1->errstr( ) . $cmd;

      my $etxt = $MAIN::DBIERROR;
      $etxt .= "\n# of Trys: $trys\nNumber of Parameters: " . ( $#para +1 ) . "\n";

      my $k;
      my $pi = 1;
      foreach $k ( @para )
      {
        $etxt .= sprintf( " Param%3d: (%3d): %s\n", $pi ++, length( $k ), $k );
      }
      $sth1->finish( );
      Errout( 'F', 157054, $etxt, $cmd );
      if ( ( $enr != 1213 &&  $enr != 1205 ) || $trys > 10 )
      {
        return;
      }
    }
  } while ( $enr eq "1213" || $enr eq "1205" );

  return $sth1;
}


# =====================================================================
#    Prepare my Sql statement
# =====================================================================

sub MySqlPrep
{
  my ( $dbh, $p, $v, $cmd ) = get_pv_args( @_ );

  my $sth1;
  my $i;

  if ( !defined( $cmd ) )
  {
    my $etxt = "[MySqlPrep]: Sql Command undefined!\n";
    $$p{ "errortext" } = $etxt;
    $$p{ "form" } = $EForm::DOCPIPE;
    Errout( 'F', 157055, $etxt, $cmd );
    dumpStack( );
    return;
  }

  if ( !defined( $dbh ) )
  {
    my $etxt = "[MySqlPrep]: dbh not defined!\n";
    $$p{ "errortext" } = $etxt;
    $$p{ "form" } = $EForm::DOCPIPE;
    Errout( 'F', 157055, $etxt, $cmd );
    dumpStack( );
    return;
  }

  unless ( $sth1 = $dbh->prepare( $cmd ) )
  {
    my $etxt = $MAIN::DBIERROR = $$p{ "errortext" } = "[MySqlPrep]: Can't prepare $cmd: [" . $dbh->err( ) . "] " . $dbh->errstr . "\n";
    $$p{ "form" } = $EForm::DOCPIPE;
    Errout( 'F', 157057, $etxt, $cmd );
    dumpStack( );
    return;
  }

  return $sth1;
}

# =====================================================================
#    Write SQL-Statement for explain
# =====================================================================
$DP::EXPLAIN_ENABLED = 0;
sub mkSql
{
  if ( !$DP::EXPLAIN_ENABLED )
  {
    return;
  }

  my ( $dbh, $p, $v, $stmt, @para ) = get_pv_args( @_ );

  if ( defined $main::EXPLAIN && $main::EXPLAIN == 1 )
  {
    return;
  }

  $main::EXPLAIN = 1;

  if ( $stmt =~ /TBL_EXPLAIN/i || $DP::holdlock > 0 || $stmt =~ /\s*UPDATE/i || $stmt !~ /^\s*select\s+/i )
  {
    $main::EXPLAIN = 0;
    return;
  }

  my $sth = MySqlExec( $dbh, $p, $v, "show tables like \"TBL_EXPLAIN\"" );
  my ( $r ) = $sth->fetchrow_array( );
  $sth->finish( );

  unless ( defined( $r ) )
  {
    $main::EXPLAIN = 0;
    return;
  }

  my $result = "";
  while( $stmt =~ /\?/ )
  {
    $result .= $`;
    $result .= "'" . shift( @para ) . "'";
    $stmt    = $';
  }

  $result .= $stmt;

  if ( $sth = MySqlExec( $dbh, $p, $v, "insert into TBL_EXPLAIN (EXPL_DATA) values (?)", $result ) )
  {
    $sth->finish( );
  }
}

# =====================================================================
#    Generate Error Message for an SQL-Statement
# =====================================================================

sub Errout {
   my $sev=shift;
   my $enr=shift;
   my $errtxt=join("\n",@_);
   my $etxt="error at: $cmd\n$errtxt\n";
   $etxt.=dumpStack();
   mkLog($sev,$enr,$etxt);
   $etxt=~ s /\r//g;
   $etxt=~ s /\n/\r/g;
#   if ($^O =~ /win/i)
#   {
#      Win32::MsgBox($etxt);
#   }
   return $sth1;
}
# =====================================================================
#    Generate Error Message
# =====================================================================

sub Errmsg {
   my $dbh=shift;
   my $errtxt=shift;
   return $errtxt;
}

# =====================================================================
#    Dump the call stack
# =====================================================================

sub dumpStack {
   my $i;
   my $res="";
   for ($i=0;$i < 20 && caller($i) && (caller($i))[2] > 0; $i++)
   {
      $res.=sprintf("%06d:[Stackdump]:     File: %s, Line: %d,   Package: %s\n",$$,(caller($i))[1],(caller($i))[2],(caller($i))[0]);
   }
   return $res;
}



# =====================================================================
#    Format the time
# =====================================================================



sub getMyTime {
   my $dbh=shift;
   my $t=shift;
   if (defined $t)
   {
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime($t);
      return sprintf("%0d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }
   if (! defined $dhb)
   {
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime(time());
      return sprintf("%0d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }
   else
   {
      $sth=MySqlExec($dbh,"SELECT UNIX_TIMESTAMP()");
      my ($lf)=$sth->fetchrow_array();
      $sth->finish();
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime($lf);
      return sprintf("%0d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }

}






sub getMyTimeStr {
   my $t=shift;
   if (defined $t)
   {
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime($t);
      return sprintf("%0d%02d%02d_%02d%02d%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }
   if (! defined $dhb)
   {
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime(time());
      return sprintf("%0d%02d%02d_%02d%02d%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }
   else
   {
      $sth=MySqlExec($dbh,"SELECT UNIX_TIMESTAMP()");
      my ($lf)=$sth->fetchrow_array();
      $sth->finish();
      my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                        localtime($lf);
      return sprintf("%0d%02d%02d_%02d%02d%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);
   }

}


# =====================================================================
#    Make a log output
# =====================================================================

sub mkLog {
   my $sev=shift;
   my $enr=shift;
   my $lg1=shift;
   my $CINA=0;
   if (defined $lg1)
   {
      unshift(@_,$lg1);
   }
   else
   {
      unshift(@_,$enr);
      unshift(@_,$sev);
      $CINA=0;
   }
   my $logmsg=join("\n",@_);
   my $ret;
   $logmsg=~ s/\s+$//;
   my $t=time();
   my $caller=sprintf('%6d:%s[%5d]: %s :: ',$$,(caller(0))[1],(caller(0))[2],getMyTime($t));
   print STDERR $caller . $logmsg . "\n";
   $ret.=$caller . $logmsg . "\n";
#    if ($CINA)
#    {
#       mkCINA($sev,$enr,$caller . $logmsg);
#    }
   return $ret;
}

# sub mkCINA {
#    my $sev=shift;
#    my $enr=shift;
#    my $logmsg=shift;
#
#
#    if ($sev =~ /D/i || $sev =~ /I/i)   { return; }
#
#
#    if ($LOG4P::init==0)
#    {
#       eval {
#          Log::Log4perl->init($ENV{'CINA_CONFIG'});
#          Log::Log4perl::MDC->put("logsource", "PRINTING");
#          Log::Log4perl::MDC->put("hostname", hostname);
#          # set traceid context
#          Log::Log4perl::MDC->put("traceid", "xxx");
#
#          # set CINA context
#          Log::Log4perl::MDC->put("cina.fehlergrp", "PRINTING");
#          Log::Log4perl::MDC->put("cina.fehlernr", "1");
#          Log::Log4perl::MDC->put("cina.fehlerart", "1");
#          my $kompid=$0;
#          $kompid=~ s/\.pl$//i;
#          $kompid=~ s/\.exe$//i;
#          $kompid=~ s/\.pm$//i;
#
#          my @pieces = split(/\//, $kompid);
#          $kompid = $pieces[-1];
# #         Log::Log4perl::MDC->put("cina.kompid", $kompid);
#          Log::Log4perl::MDC->put("cina.kompid", "DocPipe");
#          Log::Log4perl::MDC->put("cina.kompversion", "kompversion1");
#
# #         Log::Log4perl::MDC->put("cina.kompid", (caller(0))[2]);
#       } ;
#       if ($@) { warn "Problem mit Log4perl!" . $@; return; }
#       $LOG4P::init = 1;
#    }
#    Log::Log4perl::MDC->put("cina.fehlernr", $enr);
#
#    if ($sev =~ /D/i)
#    {
#       eval
#       {
#          Log::Log4perl::MDC->put("cina.fehlerart", 1 );
#          Log::Log4perl->get_logger("CINA")->debug($logmsg);
#       } ;
#       if ($@) { warn "Problem mit Log4perl!" . $@; return; }
#    }
#    elsif ($sev =~ /I/i)
#    {
#       eval
#       {
#          Log::Log4perl::MDC->put("cina.fehlerart", 2 );
#          Log::Log4perl->get_logger("CINA")->info($logmsg);
#       } ;
#       if ($@) { warn "Problem mit Log4perl!" . $@; return; }
#    }
#    if ($sev =~ /E/i)
#    {
#       eval
#       {
#          Log::Log4perl::MDC->put("cina.fehlerart", 4 );
#          Log::Log4perl->get_logger("CINA")->error($logmsg);
#       } ;
#       if ($@) { warn "Problem mit Log4perl!" . $@; return; }
#    }
#    elsif ($sev =~ /F/i)
#    {
#       eval
#       {
#          Log::Log4perl::MDC->put("cina.fehlerart", 5 );
#          Log::Log4perl->get_logger("CINA")->fatal($logmsg);
#       } ;
#       if ($@) { warn "Problem mit Log4perl!" . $@; return; }
#    }
#
# }
#

# -------------------------------------------------------------------------
# erstelle am Ende des Pfades das File Sep Zeichen
# parameter1: reference to scalar
#
sub getDir
{
  my $fld = shift;
  my $refed = $fld;

  my $ftrenn = '/';
  my $sys = OSSYS( );
  my $nw = "";


  if ( $sys == $SYS_WIN )
  {
    $ftrenn = '\\';
  }
  if ( !ref( $fld ) )
  {
    $refed = \$fld;
  }

  if ( $$refed =~ /^[\/\\]/ )
  {
    $nw = $ftrenn;
    $$refed = $';
  }
  if ( $$refed !~ /[\/\\]$/ )
  {
    $$refed .= $ftrenn;
  }
  if ( $sys == $SYS_WIN )
  {
    $$refed =~ s/\//\\/g;
    $$refed =~ s/\\+/\\/g;
  }
  else
  {
    $$refed =~ s/\\/\//g;
    $$refed =~ s/\/+/\//g;
  }
  $$refed = $nw . $$refed;
  return $$refed;
}

# -------------------------------------------------------------------------
# parameter1: reference to scalar
#
sub getPath
{
  my $fld = shift;
  my $refed = $fld;

  my $ftrenn = '/';
  my $sys = OSSYS( );
  my $nw = "";


  if ( $sys == $SYS_WIN )
  {
    $ftrenn = '\\';
  }
  if ( !ref( $fld ) )
  {
    $refed = \$fld;
  }

  if ( $$refed =~ /^[\/\\]/ )
  {
    $nw = $ftrenn;
    $$refed = $';
  }

  if ( $sys == $SYS_WIN )
  {
    $$refed =~ s/\//\\/g;
    $$refed =~ s/\\+/\\/g;
  }
  else
  {
    $$refed =~ s/\\/\//g;
    $$refed =~ s/\/+/\//g;
  }
  $$refed = $nw . $$refed;
  return $$refed;
}

# -------------------------------------------------------------------------
# erstelle ein directory
# parameter1: reference to scalar
#
sub mkDir
{
  my $fld = shift;
  my $dnam;

  if ( ref( $fld ) ) { $dnam=$$fld; }
  else               { $dnam=$fld; }

  $dnam = getDir( $dnam );

  if ( OSSYS( ) == $SYS_WIN )
  {
    if ( -e $dnam ) { return 0; }
    my $cmd = "cmd /E:ON /C mkdir \"$dnam\"";
    return system( $cmd );
  }
  else
  {
    $dnam=~ s/\\/\//g;
    if ( -e $dnam ) { return 0; }
    my $e = system( "mkdir -p \"$dnam\"" );
    system( "chmod 0777 \"$dnam\"" );
    return $e;
  }
}

sub rmDir
{
   my $fld   = shift;
   my $force = shift || 1;

   my $dnam;
   if (ref($fld)) {  $dnam=$$fld;   }
   else           {  $dnam=$fld;    }

   if ( $force == 0 && isDir_empty( $dnam ) > 0 )
   {
     if ( $main::DEBUG > 0 ) { mkLog( "D", 0, "rmDir: Directory $dnam not empty " ); }
     return;
   }

   if (OSSYS() == $SYS_WIN)
   {
      unless (-e $dnam) { return 0; }
   	  return system("rd /S /Q $dnam");
   }
   else
   {
	   $dnam=~ s/\\/\\\\/g;
     unless (-e $dnam) { return 0; }
#	  mkLog('I',157004,"mkdir -p \"$dnam\"");
	  my $e=system("rm -r $dnam");
#	  mkLog('I',157005,"chmod 0777 \"$dnam\"");
	  return $e;
   }

}

sub isDir_empty {
  my $dirnam = shift;

  if ( $dirname eq "" ) { return 0; }
  opendir( DIR, $dirnam );
  my ( @files ) = readdir( DIR );
  closedir( DIR );

  my $fcnt = 0;
  foreach my $file ( @files )
  {
    if ( $file eq "." || $file eq ".." )
    {
      next;
    }
    $fcnt++;
  }

  return $fcnt;
}
#
# pr�fe und erstelle ein Directory, wenn es noch nicht erstellt ist.

sub chkDir {
   use File::Path;
   my $fn=shift;
   my $echar;
   if ($^O =~ /win/i)   {  $echar="\\";   }
   else                 {  $echar="/";   }
   my $tchar=$echar;
   $tchar=~ s/\\/\\\\/g;
   my (@pt)=split(/$tchar/,$fn);
   if (substr($fn,-1,1) ne $echar)
   {
      pop(@pt);
   }
   my $sd;
   my $path=shift(@pt).$echar;
   my $c=0;
   foreach $sd (@pt)
   {
   	  $path.=$echar.$sd;
      $path=getDir($path);
      $c++;
      unless(-d $path)
      {
         if ($path =~ /^\\\\/ && $c<=2) {}
         else
         {
            unless (mkdir($path)) {die "Error during mkdir for $path : $!";}
         }
      }
   }
}


# =====================================================================
# Den DB_Eintrag (row) einer Task zurückgeben
# =====================================================================
sub LoadTaskVal {
   my $dbh=shift;
   my $task_id=shift;
   my $row;
   my $sth;
   unless($sth=MySqlExec($dbh,"select * from TBL_TASKD where TASKD_ID=$task_id") ) { warn "SQL-Error"; exit(DB_Error); }
   $row=$sth->fetchrow_hashref();
   $sth->finish();

   unless($sth=MySqlExec($dbh,"select * from TBL_SYV where (SYV_TASKD_TYPE=? and SYV_TASKD_ID=0) or
                                                                     (SYV_TASKD_TYPE=? and SYV_TASKD_ID=?)
                                         order by SYV_NAME,SYV_TASKD_ID desc",
                                         $row->{'TASKD_TYPE'},
                                         $row->{'TASKD_TYPE'},
                                         $task_id))
   {
      mkLog('F',157006,"SQL-Error $MAIN::DBIERROR");
      exit(DB_Error);
   }
   my $r1;
   while($r1=$sth->fetchrow_hashref())
   {
     unless (defined($row->{'TASKD_'.$r1->{'SYV_NAME'}}))
     {
       $row->{'TASKD_'.$r1->{'SYV_NAME'}}=expandVar($dbh,$r1->{'SYV_VALUE'});
     }
   }
   if ($row->{'TASKD_DEBUG'} >= 10)
   {
      foreach my $kk (keys %$row)
      {
           mkLog('I',157007,"$kk:$row->{$kk}");
      }

   }
   $sth->finish();
   return $row;
}



sub OSSYS {
   my $SYS = $^O;
   if ( $SYS =~ /mswin/i ) { return $SYS_WIN; }
   elsif ( $SYS =~ /^darwin/i ) { return $SYS_UX; }
   elsif ( $SYS =~ /^linux/i ) { return $SYS_UX; }
   elsif ( $SYS =~ /^solaris/i ) { return $SYS_UX; }
   return -1;
}

#
# prüfe und Locke ein File.
#  Rückgabewert defined    Lock war OK, Rückgabewert ist der File Handle
#               undefined  Lock ist nicht OK
sub getSyncFiles {
  my $TID=shift;
  my $CN=hostname;
  $main::lckdir=getLockDir();
  $main::restartFile="${main::lckdir}TID_${CN}_$TID.restart";
  $main::stopFile="${main::lckdir}TID_${CN}_$TID.stop";
  $main::lockFile="${main::lckdir}TID_${CN}_$TID.lock";
  $main::pidFile="${main::lckdir}TID_${CN}_$TID.pid";
  mkLog('I',157008,"\$main::restartFile=$main::restartFile");
  mkLog('I',157009,"\$main::stopFile=$main::stopFile");
  mkLog('I',157010,"\$main::lockFile=$main::lockFile");
  mkLog('I',157011,"\$main::pidFile=$main::pidFile");

}
sub clearLock {
  if (defined $main::LOCKFH)
  {
    close $main::LOCKFH;
    if (-e $main::pidFile)    { unlink($main::pidFile); }
    if (-e $main::lockFile)   { unlink($main::lockFile); }
  }
  if (-e $main::restartFile)  { unlink($main::restartFile); }
  if (-e $main::stopFile)     { unlink($main::stopFile); }
}

sub chkLock {
   my $TID=shift;
   getSyncFiles($TID);
   my $fn=$main::lockFile;     # Filename
   my $pidfn=$main::pidFile;   # Filename
   $main::LOCKFH=undef;
   STDERR->autoflush(1);
   if (! -e $fn)
   {
      unless (open (LOCK, ">$fn"))
      {
         die "open >$fn: $!";
      }
      print LOCK "$fn"; close LOCK;
   }
   unless (open (LOCK, ">>$fn"))
   {
      die "open >>$fn: $!";
   }

   my $stat;
   $stat=flock (LOCK, 2 | 4);

   if ($stat > 0    )
   {
     unless (open (PIDF, ">$pidfn"))
     {
       die "open >$pidfn: $!";
     }
     print PIDF "$$\n"; close PIDFN;
     close PIDF;
     if (-e $main::restartFile)    { unlink($main::restartFile); }
     if (-e $main::stopFile)       { unlink($main::stopFile); }
     $main::LOCKFH=*LOCK;
     $SIG{INT}   = sub { print STDERR "$$: SIG INT received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     $SIG{ALRM}  = sub { print STDERR "$$: SIG ALRM received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     $SIG{HUP}   = sub { print STDERR "$$: SIG HUP received! \nnot terminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); };
     if ($^O !~ /win/i)
     {
       $SIG{QUIT} = sub { print STDERR "$$: SIG QUIT received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     }
     $SIG{ABRT}  = sub { print STDERR "$$: SIG ABRT received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     $SIG{KILL}  = sub { print STDERR "$$: SIG KILL received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     $SIG{TERM}  = sub { print STDERR "$$: SIG TERM received! \nterminating...\n -->" . join(";",@_) . "\n"; print STDERR dumpStack(); exit(0); };
     return *LOCK;
   }
   else               { undef $main::LOCKFH; close LOCK; return undef; }
}

#
# prüfe und Locke ein File.
#  Rückgabewert defined    Lock war OK, Rückgabewert ist der File Handle
#               undefined  Lock ist nicht OK
sub getLockDir {
   my $dir;
   if (defined $ENV{'LOCKDIR'})
   {
      $dir=$ENV{'LOCKDIR'};
   }
   else
   {
      $dir=cwd;
   }
   return getDir($dir);
}

sub MySqlHinsert {
  my $dbh=shift;
  my $tb=shift;
  my $fval=shift;
  my $cmd1="insert into $tb ( ";
  my $cmd2=" values ( ";
  my @valtab;
  my $sth;
  my $iid;


  foreach my $fld (keys %$fval)
  {
    $cmd1.=$fld.", ";
    $cmd2.="?, ";
    push (@valtab,$$fval{$fld});
  }
  chop($cmd1);
  chop($cmd1);
  chop($cmd2);
  chop($cmd2);
  $cmd1.=") ";
  $cmd2.=") ";
  $cmd1.=$cmd2;

  unless($sth=MySqlExec($dbh,$cmd1,@valtab) ) { warn "SQL-Error"; return undef; }
  my $sid=$dbh->{'mysql_insertid'};
  $sth->finish();
  return $sid;
}

# =====================================================================
#    generate update statement from hash table
# =====================================================================

sub MySqlHUpdate {
  mkLog( "W", 0, "MySqlHUpdate is Deprecated! Please use MySqlHupdate!" );
  return MySqlHupdate( @_ );
}
sub MySqlHupdate
{
  my $dbh=shift;
  my $tb=shift;
  my $fval=shift;
  my $where=shift;
  my $cmd1="update $tb set ";
  my @valtab;
  my $sth;

  my @sets = ( );
  foreach my $fld ( keys %$fval )
  {
    push( @sets, "$fld = ?" );
    push ( @valtab, $$fval{ $fld } );
  }
  $cmd1 .= join( ", ", @sets );
  $cmd1 .= " $where";
  push ( @valtab, @_ );

  unless( $sth = MySqlExec( $dbh, $cmd1, @valtab ) ) { warn "SQL-Error"; return undef; }
  $sth->finish( );
  return 1;
}

sub MySqlHreplace {
  my $dbh=shift;
  my $tb=shift;
  my $fval=shift;
  my $cmd1="replace into $tb ( ";
  my $cmd2=" values ( ";
  my @valtab;
  my $sth;
  my $iid;


  foreach my $fld (keys %$fval)
  {
    $cmd1.=$fld.", ";
    $cmd2.="?, ";
    push (@valtab,$$fval{$fld});
  }
  chop($cmd1);
  chop($cmd1);
  chop($cmd2);
  chop($cmd2);
  $cmd1.=") ";
  $cmd2.=") ";
  $cmd1.=$cmd2;

  unless($sth=MySqlExec($dbh,$cmd1,@valtab) ) { warn "SQL-Error"; return undef; }
  my $sid=$dbh->{'mysql_insertid'};
  $sth->finish();
  return $sid;
}

# =====================================================================
# Die ID eines Servers zurückgeben
# =====================================================================

sub getSRVID {
   my $dbh=shift;
   my $name=shift;
   my $sth;
   my @row;

   warn "DM-Lib Version of getSRVID is deprecated! Use CASLib!";
   unless($sth=MySqlExec($dbh,"select SRV_ID from TBL_SRV where SRV_NAME='$name';"
              ) ) { mkLog('F',157012,"SQL-Error MAIN::DBIERROR"); exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];
}

# =====================================================================
# Den Status einer Task zurückgeben
# =====================================================================

sub TaskStatus {
   my $dbh=shift;
   my $task_id=shift;
   my $task_type=shift;
   my $sth;
   my @row;

   unless (defined $MAIN::cnt) {$MAIN::cnt=$task_id;}
   $MAIN::cnt++;
   if ($MAIN::cnt>=100)
   {
     if ( $DP::DEBUG > 4 ) { mkLog( 'D', 0, "vor system \"start UPdTaskMemWin32.exe $task_id $$ INIT\"" ); }
     # if (OSSYS()==$SYS_WIN) {system "start UPdTaskMemWin32.exe $task_id $$";}
     if ( $DP::DEBUG > 4 ) { mkLog( 'D', 0, "vor system \"start UPdTaskMemWin32.exe $task_id $$ INIT\"" ); }
     $MAIN::cnt=0;
   }


   unless($sth=MySqlExec($dbh,"select TASKD_STATUS from TBL_TASKD where TASKD_TYPE=? and TASKD_ID=?",
              $task_type,
              $task_id) ) { mkLog('F',157013,"SQL-Error $MAIN::DBIERROR"); exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return CAS_TERMINATE; }
   return $row[0];
}


sub ins_mem {
   my $dbh=shift;
   my $tbl=shift;
   my $v=shift;
   my $sth;
   my $vlmm;
#   my $r;
#   load_mem($dbh,$tbl,"CREATE");
   load_mem($dbh,$tbl,"");
   foreach $vlmm (keys %$v)
   {
      unless ($sth=MySqlExec($dbh,"insert ignore into $tbl values (?)",$vlmm)) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
      $sth->finish();
   }
   return 0;
}


sub load_mem {
   my $dbh=shift;
   my $tbl=shift;
   my $tbwh=shift;
   my $sth;
   my $r;
   unless ($sth=MySqlExec($dbh,"show table status like ?",$tbl)) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   $r=$sth->fetchrow_hashref();
   $sth->finish();
   if ($r->{'Rows'} eq "")
   {
      my %cmtbl;
      $cmtbl{'TBL_MEM_PRTI_KEY'}     = "create table TBL_MEM_PRTI_KEY     (primary key (MEM_PRTI_KEY))     select distinct PRTI_KEY as MEM_PRTI_KEY from TBL_PRINT_INDEX where PRTI_KEY not in(\'\$docnum\$\',\'\$preamble\$\',\'\$trailer\$\')";
      $cmtbl{'TBL_MEM_LPR_HOSTNAME'} = "create table TBL_MEM_LPR_HOSTNAME (primary key (MEM_LPR_HOSTNAME)) select distinct LPR_HOSTNAME as MEM_LPR_HOSTNAME from TBL_LPR";
      $cmtbl{'TBL_MEM_LPR_USERID'}   = "create table TBL_MEM_LPR_USERID   (primary key (MEM_LPR_USERID))   select distinct LPR_USERID as MEM_LPR_USERID from TBL_LPR order by LPR_USERID";
      $cmtbl{'TBL_MEM_LPR_STATUS'}   = "create table TBL_MEM_LPR_STATUS   (primary key (MEM_LPR_STATUS))   select distinct LPR_STATUS as MEM_LPR_STATUS from TBL_LPR order by LPR_STATUS";
      $cmtbl{'TBL_MEM_LPR_PSTAT'}    = "create table TBL_MEM_LPR_PSTAT    (primary key (MEM_LPR_PSTAT))    select distinct QED_STATUS as MEM_LPR_PSTAT from TBL_QED order by QED_STATUS";
      unless ($sth1=MySqlExec($dbh,$cmtbl{$tbl})) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
      $sth1->finish();
   }
#   elsif ($r{'Rows'} == '0' or $tbwh =~ /^REF/i)
#   {
#      if ($tbwh =~ /^CRE/i) {last;}
#      my %imtbl;
#      $imtbl{'TBL_MEM_PRTI_KEY'}     = "insert ignore into TBL_MEM_PRTI_KEY     select distinct PRTI_KEY as MEM_PRTI_KEY from TBL_PRINT_INDEX where PRTI_KEY not in(\'\$docnum\$\',\'\$preamble\$\',\'\$trailer\$\')";
#      $imtbl{'TBL_MEM_LPR_HOSTNAME'} = "insert ignore into TBL_MEM_LPR_HOSTNAME select distinct LPR_HOSTNAME as MEM_LPR_HOSTNAME from TBL_LPR";
#      $imtbl{'TBL_MEM_LPR_USERID'}   = "insert ignore into TBL_MEM_LPR_USERID   select distinct LPR_USERID as MEM_LPR_USERID from TBL_LPR order by LPR_USERID";
#      $imtbl{'TBL_MEM_LPR_STATUS'}   = "insert ignore into TBL_MEM_LPR_STATUS   select distinct LPR_STATUS as MEM_LPR_STATUS from TBL_LPR order by LPR_STATUS";
#      unless ($sth1=MySqlExec($dbh,$imtbl{$tbl})) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
#      $sth1->finish();
      if ($tbl =~ /_MEM_LPR_STATUS$/)
      {
         my $v;
         foreach $v (keys %LPR::DEF)
         {
            unless ($sth1=MySqlExec($dbh,"insert ignore into $tbl values (?)",$LPR::DEF{$v})) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
            $sth1->finish();
         }

         $tbl=TBL_MEM_LPR_PSTAT;
         foreach $v (keys %QEDSTAT::DEF)
         {
            unless ($sth1=MySqlExec($dbh,"insert ignore into $tbl values (?)",$QEDSTAT::DEF{$v})) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
            $sth1->finish();
         }
      }
#   }
}

# =====================================================================
# Die ID einer Instanz zurückgeben
# =====================================================================

sub getINSTID {
   my $dbh=shift;
   my $inst_name=shift;
   my $sth;
   my @row;

   unless($sth=MySqlExec($dbh,"select INST_ID from TBL_INSTANCE where INST_NAME='$inst_name';"
              ) ) { mkLog('F',157034,"SQL-Error $MAIN::DBIERROR"); exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];
}

# =====================================================================
# Die ID eines Mandanten zurückgeben
# =====================================================================

sub getMANDID {
   my $dbh=shift;
   my $mand_name=shift;
   my $sth;
   my @row;

   unless($sth=MySqlExec($dbh,"select MAND_ID from TBL_MAND where MAND_NAME='$mand_name';"
              ) ) { mkLog('F',157035,"SQL-Error MAIN::DBIERROR"); exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];
}



# =====================================================================
# Die ID einer Applikation  zurückgeben
# =====================================================================

sub getAPPLID {
   my $dbh=shift;
   my $mandnm=shift;
   my $applnm=shift;
   my $sth;
   my @row;

   my $sql_cmd = "";
   if ($mandnm eq "")
   {
     $sql_cmd = "select APPLC_ID from TBL_APPLCONF where APPLC_MAND_ID=0 and APPLC_NAME=\"$applnm\"";
   }
   else
   {
     $sql_cmd = "select APPLC_ID from TBL_APPLCONF,TBL_MAND where MAND_NAME=\"$mandnm\" and APPLC_MAND_ID=MAND_ID or APPLC_MAND_ID=0 and APPLC_NAME=\"$applnm\"";
   }

   unless ($sth=MySqlExec($dbh,$sql_cmd)) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];

}

# =====================================================================
# Ermitteln der Werte aus PQUE und PTYP
# =====================================================================

sub getQUEVars
{
  my $dbh = shift;
  my $HTab = shift;
  my $pque_id = shift;
  my $sth;
  my @row;
  my $kk;
  unless ( $sth = MySqlExec( $dbh, "select * from TBL_PQUE where PQUE_ID=?", $pque_id ) )
  {
    mkLog( "F", 0, "SQL-Error $MAIN::DBIERROR" );
    exit( DB_Error );
  }
  $row = $sth->fetchrow_hashref( );
  foreach $kk ( keys %$row )
  {
    $$HTab{ $kk } = $row->{ $kk };
  }
  $sth->finish( );

  my $ptyp = $row->{ 'PQUE_PRTYP_ID' };
  getPTYPVars( $dbh, $HTab, 0, 0 );
  getPTYPVars( $dbh, $HTab, 0, $ptyp );
  getPTYPVars( $dbh, $HTab, $pque_id, $ptyp );

  $$HTab{ "PTYP_OUTDSTR_TXT" } = $DTYPE::DEF[ $$HTab{ "PTYP_OUTDSTR" } ];
}

sub getPTYPVars {
   my $dbh=shift;
   my $HTab=shift;
   my $pque_id=shift;
   my $ptyp_id=shift;
   my $sth;
   my @row;
   warn "getPTYPVars is deprecated - use CASLib-Version!";
   if (! defined $pque_id || $pque_id eq "")
   {
      $pque_id=0;
   }
   unless ($sth=MySqlExec($dbh,"select * from TBL_PTYP where PTYP_ID=? and PTYP_PQUE_ID=?",$ptyp_id,$pque_id)) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   $row=$sth->fetchrow_hashref();
   foreach $kk (keys %$row)
   {
     if (defined ($row->{$kk}) or $ptyp_id+$pque_id==0)
     {
       my $kc=$kk;
       $kc=~ s/^PTYP/PQUE/i;
       $$HTab{$kc}=expandVar($dbh,$row->{$kk});
	  }
   }

}

# =====================================================================
# Die ID eines PRTYPS  zurückgeben
# =====================================================================

sub getPRTYPID {
   my $dbh=shift;
   my $prtyp=shift;
   my $sth;
   my @row;
   unless ($sth=MySqlExec($dbh,"select PTYP_ID from TBL_PTYP where PTYP_PRTYP=\"$prtyp\"")) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];

}

# =====================================================================
# Der Name einer PRTID  zurückgeben
# =====================================================================

sub getPRTYP {
   my $dbh=shift;
   my $prtyp=shift;
   my $sth;
   my @row;
   unless ($sth=MySqlExec($dbh,"select PTYP_PRTYP from TBL_PTYP where PTYP_ID=\"$prtyp\"")) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];

}


# =====================================================================
# Die ID einer Applikation  zurückgeben
# =====================================================================

sub getQUEID {
   my $dbh=shift;
   my $quenm=shift;
   my $sth;
   my @row;
   unless ($sth=MySqlExec($dbh,"select PQUE_ID from TBL_PQUE where PQUE_SRC=\"$quenm\"")) { warn "SQL-Error\n$MAIN::DBIERROR\n"; exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return 0; }
   return $row[0];

}



# =====================================================================
# Den Namen eines Mandanten zurückgeben
# =====================================================================

sub getMANDNAM {
   my $dbh=shift;
   my $mand_id=shift;
   my $sth;
   my @row;

   if ($mand_id==0) {return "GLOBAL";}
   unless($sth=MySqlExec($dbh,"select MAND_NAME from TBL_MAND where MAND_ID=?;",
              $mand_id) )
   {
      mkLog('F',157036,"SQL-Error $MAIN::DBIERROR"); exit(DB_Error);
   }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return "-"; }
   return $row[0];
}

# =====================================================================
# Den Namen eines Servers zurückgeben
# =====================================================================

sub getSRVNAM {
   my $dbh=shift;
   my $id=shift;
   my $sth;
   my @row;

   if ($id==0) {return "GLOBAL";}
   unless($sth=MySqlExec($dbh,"select SRV_NAME from TBL_SRV where SRV_ID=?;",
              $id) )
   {
      mkLog('F',157037,"SQL-Error $MAIN::DBIERROR"); exit(DB_Error);
   }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return "-"; }
   return $row[0];
}



# =====================================================================
# Den Namen einer Instanz zurückgeben
# =====================================================================

sub getINSTNAM {
   my $dbh=shift;
   my $inst_id=shift;
   my $sth;
   my @row;

   if ($inst_id==0) {return "MASTER";}
   unless($sth=MySqlExec($dbh,"select INST_NAME from TBL_INSTANCE where INST_ID=?;",
              $inst_id) ) { mkLog('F',157038,"SQL-Error $MAIN::DBIERROR"); exit(DB_Error); }
   @row=$sth->fetchrow_array();
   $sth->finish();
   if (! defined ($row[0])) { return "-"; }
   return $row[0];
}


# =====================================================================
#    mit der Datenbank verbinden und den handle zurückgeben
# =====================================================================


sub DBConnect
{
  my $db     = shift || $ENV{ "DBCSTR" };
  my $dbuser = shift || $ENV{ DBUSER } || $ENV{ USER } || "root";
  my $dbpw   = shift || $ENV{ DBPW } || $ENV{ PW } || "";
  my %opts   = @_;

  trim( \$dbuser );
  trim( \$dbpw );
  my $tries = $opts{ "tries" } || 300;
  my $tryi = 1;
  my $i;
  my $dbh;

  warn "db: $db";
  warn "dbuser: $dbuser";
  warn "dbpw: $dbpw";

  if ( $db eq "" )
  {
    die "Can't get database connectionstring from environmentvariable DBCSTR!\n$MAIN::DBIERROR\n";
  }

  if ( $db !~ /mysql_local_infile/i )
  {
    $db .= ";mysql_local_infile=1";
  }

  for ( $i = 1; $i <= $tries; $i = $i + $tryi )
  {
    unless ( $dbh = DBI->connect( $db, $dbuser, $dbpw ) )
    {
      mkLog( 'I', 157039, join( "\n", @_ ) );
      mkLog( 'E', 157042, "Database connection error: $MAIN::DBIERROR - retry $i\n" );
      sleep( $tryi );
    }
    else
    {
      $dbh->{ 'AutoCommit' } = 1;
      $dbh->do( 'set character_set_client="latin1"' );
      $dbh->do( 'set character_set_results="latin1"' );
      $dbh->do( 'set character_set_connection="latin1"' );
      $dbh->do( "set innodb_lock_wait_timeout=2000" );

      return $dbh;
    }
  }
  warn "Database connection error\n$MAIN::DBIERROR\n";
  return undef;
}

#
# -------------------------------------------------------
# Ausgabe mit Fehlerpr�fung schreiben
# -------------------------------------------------------
#
sub doPrint {
   my $fh=shift;           # Filehandle holen
   foreach my $l (@_)
   {
      if (ref($l) eq 'SCALAR')
      {
         unless(print $fh $$l)
         {
            print STDERR dumpStack();
            die mkLog("F",999001,"Daten können nicht korrekt geschrieben werden!\n$!");
         }
      }
      elsif (ref($l) eq 'ARRAY')
      {
         unless(print $fh @{$l})
         {
            print STDERR dumpStack();
            die mkLog("F",999002,"Daten können nicht korrekt geschrieben werden!\n$!");
         }
      }
      elsif (ref($l) eq 'HASH')
      {
         unless(print $fh %{$l})
         {
            print STDERR dumpStack();
            die mkLog("F",999003,"Daten können nicht korrekt geschrieben werden!\n$!");
         }
      }
      elsif (ref($l) eq 'CODE')
      {
         unless(print $fh &{$l})
         {
            print STDERR dumpStack();
            die mkLog("F",999004,"Daten können nicht korrekt geschrieben werden!\n$!");
         }
      }
      else
      {
         unless(print $fh $l)
         {
            print STDERR dumpStack();
            die mkLog("F",999005,"Daten können nicht korrekt geschrieben werden!\n$!");
         }
      }
   }
}
# ----------------------------------------------------------------------------------------
sub mem {
   unless ($^O=~ /win32/i)     {return 0;}
   unless (defined $MAIN::WMI) {return 0;}
   my $ProcList = $MAIN::WMI->InstancesOf( "Win32_Process" );
   foreach my $Proc (in( $ProcList )  )
   {
     if ($Proc->{ProcessID}==$$)
     {
       return $Proc->{WorkingSetSize};
       last;
     }
   }
}


sub mem_limit {
   unless (defined $DP::memstart) {$DP::memstart=mem();}
   my $mem=mem();
   if ($mem==0) {return 0;}
   my $mr=$DP::memstart*1.3;
   my $per=int($mem/$DP::memstart*100);
   mkLog('F',157040,"mem: Init:$DP::memstart Current:$mem = $per\%");
   if ($DP::memstart*1.3<$mem)
   {
      return 1;
       mkLog('I',157041,"Restart of Task because memory increased over 30 percent Init:$DP::memstart Current:$mem");
   }
   return 0;
}


sub leakRestart
{
  $DP::restartChecks ++;

  if ($DP::restartChecks >= 100)
  {
    return 1;
  }
  my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
					    localtime(time());
  if ($DP::restartHour == -1)
  {
    $DP::restartHour = $hour;
  }
  if ($DP::restartHour != $hour)
  {
    return 1;
  }
  return 0;
}

sub clean_xml
{
  my $str = shift;
  $str = HTML::Entities::encode( $str );
  return $str;
}

sub clean_file_name {
  my $str1=shift;
  $str1 =~ tr/\\\/ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ/\\\/AAAAAAACEEEEIIIIDNOOOOOOUUUUYbsaaaaaaaceeeeiiiidnoooooouuuuyby/;
  $str1 =~ tr/\\\/\.a-zA-Z0-9/_/cs;
  return $str1;
}


sub clean_cs_dpss
{
  my $str = shift;
  $str =~ s/\^</</g;
  $str =~ s/\^>/>/g;
  $str =~ s/\^\^/^/g;
  $str =~ s/\^\~/~/g;
  return $str;
}

# This method can be used to get the temporary directory.
sub get_tempdir
{
  my $tmpdir = $ENV{ "TEMP" };
  if ( $tmpdir eq "" )
  {
    $tmpdir = $ENV{ "TMP" };
  }
  if ( $tmpdir eq "" )
  {
    $tmpdir = "/tmp";
  }
  return $tmpdir;
}

# ================================================================================
# Dupletten und leere Teile entfernern
sub cleanMailAdr {
  my $ma=shift;

  my %mh=();
  my @maerg;
  my (@madr)=split(/[;,\t]/,$ma);

  foreach my $adr ( @madr )
  {
     $adr =~ s/^\s+//;
     $adr =~ s/^\s+//;
     if ( $adr eq "" ) { next; }
     my $tadr=$adr;
     # if ( $adr =~ /"[^"]*"/ )
     # {
     #    $tadr="$`$'";
     # }
     if ( $tadr =~ /([_A-Za-z0-9\.-]+\@[_A-Za-z0-9\.-]+\.[_A-Za-z0-9]{2,})/ )
     {
        my $ucadr=uc($1);
        if ( defined $mh{$ucadr} ) { next; }
        $mh{$ucadr} = $ucadr;
        push(@maerg,$1);
     }
  }
  return join(";",@maerg);
}

sub get_pv_args
{
  my @args = @_;

  if ( $#args >= 0 && ref( $args[ 0 ] ) eq "DBI::db" )
  {
    # At least dbh was defined
    if ( $#args >= 2 && ref( $args[ 1 ] ) eq "HASH" && ref( $args[ 2 ] ) eq "HASH" )
    {
      # $p and $v are defined
      return @args;
    }
    elsif ( $#args >= 2 && ref( $args[ 1 ] ) eq "HASH" && !defined $args[ 2 ] )
    {
      # $p defined, $v undefined
      return ( shift( @args ), shift( @args ), shift( @args ) || { }, @args );
    }
    else
    {
      # No $p and $v are defined
      return ( shift( @args ), { }, { }, @args );
    }
  }
  else
  {
    # No dbh was given - try to get one...
    return ( DBConnect( ), { }, { }, @args );
  }
}

sub get_cstmr_by_cid
{
  my ( $dbh, $p, $v, $cid ) = ( @_ );

  $cid = $cid || $$p{ 'cid' };


  my $sth;
  my $cust_data;

  unless ( $sth = Apache::DOCPIPE::MySqlExec(
                    $dbh, $p, $v,
                    "select DPC_KEY, DPC_NAME, DPC_MAND_ID, DPC_ENCR_PRIV, DPC_ENCR_PUB, MAND_NAME, MAND_ID from TBL_DOCPRINT_CUSTOMERS, TBL_MAND WHERE DPC_MAND_ID = MAND_ID and DPC_KEY = ?",
                    $cid
                  )
         )
  {
    Apache::DOCPIPE::mkLog( "F", 0, "SQL-Error!" );

    $$v{ 'APPERROR' } = Apache::DOCPIPE::GetCaption( $dbh, $p, $v, "EMSG0999" );
    $$v{ "ResponseFile" } = "docprint/error.html";
    return;
  }
  $cust_data = $sth->fetchrow_hashref( );
  Apache::DOCPIPE::mkLog("I", 0, "CUSTDATA1: " . Dumper $cust_data);

  $sth->finish( );
  return $cust_data;
}
sub decrypt_username
{
  my ( $dbh, $p, $v, $cid, $enc_username ) = ( @_ );


  my $cust_data = main::get_cstmr_by_cid( $dbh, $p, $v, $cid );

  my $rsa_enc_username = decode_base64( uri_unescape( $enc_username ) );


  my $rsa_priv = Crypt::OpenSSL::RSA->new_private_key( $$cust_data{ "DPC_ENCR_PRIV" }  );

  $rsa_priv->use_pkcs1_padding( );

  my $decrypt_max_size = $docPRINT::decrypt_size;

  my $username;
  my $sub_text;
  my $sub_username;

  my $packet_count = (length ( $rsa_enc_username ) / $docPRINT::decrypt_size);

  my $template = "a$docPRINT::decrypt_size" x $packet_count;
  for ( my $i = 0; $i < length ( $rsa_enc_username ); $i += $decrypt_max_size )
  {
    Apache::DOCPIPE::mkLog( "I", 0,  "DECRYPT STEP: " . $i );
    $sub_text = substr( $rsa_enc_username, $i, $decrypt_max_size);
    $sub_username = $rsa_priv->decrypt( $sub_text );

    $username .= $sub_username;
  }
  return $username;
}

sub create_qed_stati_key_map
{
  my ( $dbh, $p, $v, $qed_stati ) = ( @_ );

  foreach my $status ( @$qed_stati )
  {
    $status = {
      "key" => $status,
      "value" => Apache::DOCPIPE::GetCaption( $dbh, $p, $v, 'CAP_QED_STATUS'. $status )
    };
  }
}

sub get_time_sec
{
  my $time_str = shift;

  if ( $time_str =~ /([0-9]+)/ )
  {
    my $sec = $1;
    my $unit = $';

    if ( $unit =~ /h/i ) { $sec = $sec * 60 * 60; }
    elsif ( $unit =~ /m/i ) { $sec = $sec * 60; }
    elsif ( $unit =~ /t/i ) { $sec = $sec * 60 * 60 * 24; }
    elsif ( $unit =~ /d/i ) { $sec = $sec * 60 * 60 * 24; }

    return $sec;
  }
  return undef;
}
# returns all 'Mandanten' (ID and Name)
sub getAllMands
{
  my ( $dbh, $p, $v ) = ( @_ );

  my $sql_cmd = "SELECT MAND_ID, MAND_NAME FROM TBL_MAND";

  my $sth;

  unless ( $sth = Apache::DOCPIPE::MySqlExec( $dbh, $p, $v, $sql_cmd ) )
  {
    Apache::DOCPIPE::mkLog( "F", 0, "SQL-Error!" );

    $$v{ 'APPERROR' } = Apache::DOCPIPE::GetCaption( $dbh, $p, $v, "EMSG0999" );
    return;
  }

  my @mands = ();

  while ( my $mand = $sth->fetchrow_hashref() )
  {
    push ( @mands, $mand );
  }
  $sth->finish();

  return \@mands;
}

# returns all instances (ID and NAME)
sub getAllInstances
{
  my ( $dbh, $p, $v ) = ( @_ );

  my $sql_cmd = "SELECT INST_ID, INST_NAME FROM TBL_INSTANCE";

  my $sth;

  unless ( $sth = Apache::DOCPIPE::MySqlExec( $dbh, $p, $v, $sql_cmd ) )
  {
    Apache::DOCPIPE::mkLog( "F", 0, "SQL-Error!" );

    $$v{ 'APPERROR' } = Apache::DOCPIPE::GetCaption( $dbh, $p, $v, "EMSG0999" );
    return;
  }

  my @instances = ();

  while ( my $instance = $sth->fetchrow_hashref() )
  {
    push ( @instances, $instance );
  }
  $sth->finish();

  return \@instances;
}



# This method can be used to update the qed_status of the passed qed_ids
# @param $dbh The database handle.
# @param $p
# @param $v
# @param $qed_ids The specific qed_ids, which should be updated
# @param $qed_status defines, which status receive the passed qed_ids
sub update_qed_status
{
  my ( $dbh, $p, $v, $qed_ids, $qed_status ) = ( @_ );

  my $placeholders = join ( ",", ( "?" ) x @{ $qed_ids } );

  my $sql = "update TBL_QED set QED_STATUS=? where QED_ID in ($placeholders)";
  my $sth;
  unless ( $sth = MySqlExec(
      $dbh, $p, $v,
      $sql,
      $qed_status,
      @{ $qed_ids }
    )
  )
  {
    $$v{ 'APPERROR' } = GetCaption( $dbh, $p, $v, "EMSG0999" );
    return undef;
  }

  $sth->finish( );
  return 1;
}

1;
