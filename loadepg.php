<?php
Define('FB_HOST', 'localhost');  // IP адрес сервера Firebird
Define('FB_DB', 'A4on');       // алиас базы из файла aliases.conf
Define('FB_USER', 'SYSDBA');     // Имя пользователя БД
Define('FB_PASS', 'masterkey');  // Пароль БД

ini_set('memory_limit', '1024M');
ini_set('max_execution_time', '0');

$db_session = new fbsql_db(FB_HOST, FB_DB, FB_USER, FB_PASS, 'UTF8');

$sql = 'select * from epg_sources where coalesce(HAND_ONLY,0) = 0 order by name';
$res = $db_session->select_assoc_all($sql);

foreach ($res as $src) {
    echo date('H:i:s') . ' load ' . $src['NAME'] . PHP_EOL;
    switch ($src['PARSEAS']) {
        case 0:
            ParseAsXML($db_session, $src);
            break;

        case 1:
            // ParseAsA4on($db_session, $src);
            break;
    }
}
// удалим старое расписание
$res = $db_session->exec_modify_sql('delete from Epg where Epg_Date < dateadd(day,  -8, current_date);');

function ParseAsXML($db_session, $src)
{
    $week_file = file_get_contents($src['LOCAL_FILE']);

    $xml = simplexml_load_string($week_file);
    $json = json_encode($xml);
    $xml = json_decode($json, TRUE);
    $chMap = BuildChannelMap($db_session, $src);
    $geMap = BuildGenreMap($db_session, $src);
    $ch_dates = array();
    $epg = array();
    $tz_offset = get_timezone_offset();
    foreach ($xml['programme'] as $ec_row) {
        $ch = $ec_row['@attributes']['channel'];

        if (!array_key_exists($ch, $chMap)) continue;

        $ch_id = $chMap[$ch];
        $row['ch_id'] = $ch_id;
        $row['title'] = GetXmlNode($ec_row, 'title');
        $sub = GetXmlNode($ec_row, 'sub-title');
        if ($sub <> '') $sub = $sub . '. ';
        $row['description'] = $sub . GetXmlNode($ec_row, 'desc');
        $row['date_start'] = GetXmlDateTime($ec_row['@attributes']['start']);
        $row['date_stop'] = GetXmlDateTime($ec_row['@attributes']['stop']);
        $row['actors'] = GetXmlNode($ec_row, 'actors');
        $row['country'] = GetXmlNode($ec_row, 'country');
        $row['create_year'] = GetXmlNode($ec_row, 'year');
        $row['directed'] = GetXmlNode($ec_row, 'directed');
        $row['dvbgenres'] = GetXmlNode($ec_row, 'dvbganre');
        $row['genres'] = GetXmlNode($ec_row, 'category');
        if ($row['dvbgenres'] == '')
        if (array_key_exists($row['genres'], $geMap)) $row['dvbgenres'] = $geMap[$row['genres']];
        $row['minage'] = str_replace('+', '', GetXmlNode($ec_row, 'parental'));
        if ($row['minage'] == '') {
            $str = $row['title'];
            if (preg_match("/((\\(|\\[){1,2}(\\d{1,2})\\+(\\)|\\]){1,2})/U", $str, $matches)) {
                $row['title'] = str_replace($matches[0], '', $row['title']);
                $row['minage'] = $matches[3];
            } else {
                if (array_key_exists('rating', $ec_row)) {
                    if (array_key_exists('value', $ec_row['rating'])) $str = $ec_row['rating']['value'];
                    else $str = $ec_row['rating'];
                    if (preg_match("/(\\d{1,2})\\+/U", $str, $matches)) $row['minage'] = $matches[1];
                }
            }
        }
        $row['utc_start'] = $row['date_start'] - $tz_offset;
        $row['utc_stop'] = $row['date_stop'] - $tz_offset;

        $epg[] = $row;

        $ch_dates[$ch_id][date('Y-m-d', $row['date_start'])] = $ch_id;
    }

    ClearEpgForDate($db_session, $ch_dates);
    AddEpgToDB($db_session, $epg);
    UpdateEPGLoadTime($db_session, $ch_dates);
}

function GetXmlNode($xml, $node)
{
    if (array_key_exists($node, $xml))
    if (!is_array($xml[$node])) return $xml[$node];
    return '';
}

function GetXmlDateTime($strdate)
{
    //20151219020000 +0000
    $v = explode(' ', $strdate);
    $t = $v[0];
    $y = substr($t, 0, 4);
    $m = substr($t, 4, 2);
    $d = substr($t, 6, 2);
    $h = substr($t, 8, 2);
    $mi = substr($t, 10, 2);
    return mktime($h, $mi, 0, $m, $d, $y);
}

function ClearEpgForDate($db_session, $dates)
{
    echo date('H:i:s') . ' clear' . PHP_EOL;
    foreach ($dates as $row) {
        foreach ($row as $d => $c) {
            $sql = "delete from EPG where CH_ID = $c and EPG_DATE = '$d'";
            $res = $db_session->exec_modify_sql($sql);
        }
    }
}

function AddEpgToDB($db_session, $epg)
{
    echo date('H:i:s') . ' add epg' . PHP_EOL;
    foreach ($epg as $row) {
        $Ch_Id = $row['ch_id'];
        $Title = StrToNull($row['title']);
        $Description = StrToNull($row['description'], 4096);
        $Date_Start = StrToDateTime($row['date_start']);
        $Date_Stop = StrToDateTime($row['date_stop']);
        $Actors = StrToNull($row['actors']);
        $Country = StrToNull($row['country']);
        $Create_Year = StrToNull($row['create_year']);
        $Directed = StrToNull($row['directed']);
        $Dvbgenres = StrToNull($row['dvbgenres']);
        $Genres = StrToNull($row['genres']);
        $Minage = StrToNull($row['minage']);
        $Utc_Start = StrToDateTime($row['utc_start']);
        $Utc_Stop = StrToDateTime($row['utc_stop']);

        $sql = "execute procedure Epg_Add($Ch_Id, $Title, $Utc_Start, $Utc_Stop, $Date_Start, $Date_Stop, $Description, $Genres, $Dvbgenres, $Minage, $Create_Year, $Actors, $Directed, $Country);";
        $res = $db_session->exec_modify_sql($sql);
    }
}

function BuildChannelMap($db_session, $src)
{
    $sql = "select EPG_CODE, CH_ID from epg_mapping where epg_id = {$src['ID']}";
    $res = $db_session->select_assoc_all($sql);
    $map = array();
    foreach ($res as $r) {
        $map[$r['EPG_CODE']] = $r['CH_ID'];
    }
    return $map;
}

function BuildGenreMap($db_session, $src)
{
    $sql = "select GENRE_ID, SOURCE_GENRE from epg_mapping_genre where epg_id = {$src['ID']}";
    $res = $db_session->select_assoc_all($sql);
    $map = array();
    foreach ($res as $r) {
        $map[$r['SOURCE_GENRE']] = $r['GENRE_ID'];
    }
    return $map;
}

function UpdateEPGLoadTime($db_session, $dates)
{
    echo date('H:i:s') . ' update epg' . PHP_EOL;
    $s = '';
    foreach ($dates as $row)
    foreach ($row as $c) $s .= $c . ',';

    if ($s <> '') {
        $s .= '-999';
        $sql = 'update Dvb_Streams set Epg_Updated = current_timestamp where Dvbs_Id in (select distinct Dvbs_Id from Dvb_Stream_Channels where Ch_Id in (' . $s . '))';
        $res = $db_session->exec_modify_sql($sql);
    }
}

function get_timezone_offset()
{
    $tz = date_default_timezone_get();
    return timezone_offset_get(new DateTimeZone($tz), new DateTime());
}

function StrToNull($s, $len = 255)
{
    if ($s == '') {
        return 'null';
    } else {
        $s = str_replace("'", "''", $s);
        $s = str_replace("\n", " ", $s);
        $s = mb_substr($s, 0, $len, 'utf-8');
        return "'" . $s . "'";
    }
}

function StrToDateTime($s)
{
    if ($s == '') return 'null';
    else return date("'Y-m-d H:i:s'", $s);
}

class fbsql_db
{
    var $session;
    var $debug = false;
    function __construct($sqlserver, $database, $sqluser = 'SYSDBA', $sqlpassword = 'masterkey', $charset = 'UTF8')
    {
        $this->user     = $sqluser;
        $this->password = $sqlpassword;
        $this->server   = $sqlserver;
        $this->dbname   = $database;
        $this->session = ibase_connect($this->server . ":" . $this->dbname, $this->user, $this->password, $charset);
        return $this->session;
    }
    
    // удаление объекта
    function __destruct()
    {
        if ($this->session) {
            $result = @ibase_close($this->session);
            return $result;
        } else {
            return false;
        }
    }
    
    // Выборка всего результата в ассоциативный массив
    function select_assoc_all($sql = "")
    {
        if ($sql != "") {
            if ($this->debug) {
                echo $sql . PHP_EOL;
            }
            $transaction = ibase_trans(IBASE_READ | IBASE_COMMITTED | IBASE_NOWAIT, $this->session);
            $statement = ibase_query($transaction, $sql);
            $result = array();
            $i = 0;
            while ($row = ibase_fetch_assoc($statement)) {
                $result[$i] = $row;
                $i++;
            }
            ibase_free_result($statement);
            ibase_rollback($transaction);
            return $result;
        } else {
            return FALSE;
        }
    }
    
    // Выполнение UPDATE или INSERT инструкций
    function exec_modify_sql($sql = "")
    {
        if ($sql != "") {
            if ($this->debug) echo $sql . PHP_EOL;
            $transaction = ibase_trans(IBASE_WRITE | IBASE_NOWAIT, $this->session);
            $statement = ibase_query($transaction, $sql);
            if ($statement) ibase_commit($transaction);
            return $statement;
        } else {
            return FALSE;
        }
    }

    function close()
    {
        if ($this->session) {
            $result = @ibase_close($this->session);
            return $result;
        } else {
            return false;
        }
    }
}
