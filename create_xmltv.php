<?php

Define('FB_HOST', '127.0.0.1');
Define('FB_DB', 'A4on_DB');
Define('FB_USER', 'SYSDBA');
Define('FB_PASS', 'masterkey');

Define('TIME_SHIFT', 3);
Define('XML_DIR', __DIR__ . '/');

ini_set('memory_limit', '1024M');
ini_set('max_execution_time', '0');

$db_session = new fbsql_db(FB_HOST, FB_DB, FB_USER, FB_PASS, 'UTF8');

$sql = 'select D.DVBS_ID, D.TSID, D.NAME, S.LCN, C.CH_ID, C.CH_NAME
        from Dvb_Streams d 
          inner join Dvb_Stream_Channels s on (d.Dvbs_Id = s.Dvbs_Id)
          inner join channels c on (s.Ch_Id = c.Ch_Id)
        order by d.Name';
$res = $db_session->select_assoc_all($sql);

$chnls = array();
$xml = '';

foreach ($res as $row) {
    $chnls[] = array(
        'DVBS_ID' => $row['DVBS_ID'],
        'TSID' => $row['TSID'],
        'NAME' => $row['NAME'],
        'CH_ID' => $row['CH_ID'],
        'CH_NAME' => $row['CH_NAME'],
    );
    $xml .= '<channel id="' . $row['CH_ID'] . '"><display-name lang="ru">' . $row['CH_NAME'] . '</display-name></channel>' . PHP_EOL;
}

$start = date('Y-m-d');
$end = date('Y-m-d', strtotime('+7 day'));

$tz = str_pad(TIME_SHIFT, 2, '0', STR_PAD_LEFT);

foreach ($chnls as $chnl) {
    $sql = "SELECT DATE_START, DATE_STOP, TITLE, DESCRIPTION, coalesce(Minage,0) MINAGE --, GENRES, DVBGENRES, CREATE_YEAR, ACTORS, DIRECTED, COUNTRY, LANG
    FROM GET_EPG({$chnl['DVBS_ID']}, '$start', '$end', 0)
    WHERE CH_ID = {$chnl['CH_ID']}
    ORDER BY DATE_START";
    $res = $db_session->select_assoc_all($sql);

    foreach ($res as $r) {
        $sd = FBDateTimetoUnix($r['DATE_START']) + TIME_SHIFT * 60 * 60;
        $ed = FBDateTimetoUnix($r['DATE_STOP']) + TIME_SHIFT * 60 * 60;

        $xml .= '<programme start="' . date('YmdHis', $sd) . ' +'.$tz.'00" stop="' . date('YmdHis', $ed) . ' +'.$tz.'00" channel="' . $chnl['CH_ID'] . '">' . PHP_EOL;
        $xml .= '  <title lang="ru">' . htmlspecialchars($r['TITLE']) . '</title>' . PHP_EOL;
        $xml .= '  <desc lang="ru">' . htmlspecialchars($r['DESCRIPTION']) . '</desc>' . PHP_EOL;
        if ($r['MINAGE'] != '0') {
            $xml .= '  <rating>' . $r['MINAGE'] . '</rating>' . PHP_EOL;
        }
        $xml .= '</programme>' . PHP_EOL;
    }
}
$fn = XML_DIR . 'epg.xml';
file_put_contents($fn, '<?xml version="1.0" encoding="UTF-8"?><tv>' . PHP_EOL . $xml . '</tv>');

function FBDateTimetoUnix($t)
{
    //2016-12-22 21:15:00
    $y = substr($t, 0, 4);
    $m = substr($t, 5, 2);
    $d = substr($t, 8, 2);
    $h = substr($t, 11, 2);
    $i = substr($t, 14, 2);

    return mktime($h, $i, 0, $m, $d, $y);
}

class fbsql_db
{
    public $session;
    public $debug = false;
    public function __construct($sqlserver, $database, $sqluser = 'SYSDBA', $sqlpassword = 'masterkey', $charset = 'UTF8')
    {
        $this->user = $sqluser;
        $this->password = $sqlpassword;
        $this->server = $sqlserver;
        $this->dbname = $database;
        $this->session = ibase_connect($this->server . ':' . $this->dbname, $this->user, $this->password, $charset);

        return $this->session;
    }

    // удаление объекта
    public function __destruct()
    {
        if ($this->session) {
            $result = @ibase_close($this->session);

            return $result;
        } else {
            return false;
        }
    }

    // Выборка всего результата в ассоциативный массив
    public function select_assoc_all($sql = '')
    {
        if ($sql != '') {
            if ($this->debug) {
                echo $sql . PHP_EOL;
            }
            $transaction = ibase_trans(IBASE_READ + IBASE_COMMITTED + IBASE_NOWAIT, $this->session);
            $statement = ibase_query($transaction, $sql);
            $result = array();
            $i = 0;
            while ($row = ibase_fetch_assoc($statement)) {
                $result[$i] = $row;
                ++$i;
            }
            ibase_free_result($statement);
            ibase_rollback($transaction);

            return $result;
        } else {
            return false;
        }
    }

    // Выполнение UPDATE или INSERT инструкций
    public function exec_modify_sql($sql = '')
    {
        if ($sql != '') {
            if ($this->debug) {
                echo $sql . PHP_EOL;
            }
            $transaction = ibase_trans(IBASE_WRITE + IBASE_NOWAIT, $this->session);
            $statement = ibase_query($transaction, $sql);
            if ($statement) {
                ibase_commit($transaction);
            }

            return $statement;
        } else {
            return false;
        }
    }

    public function close()
    {
        if ($this->session) {
            $result = @ibase_close($this->session);

            return $result;
        } else {
            return false;
        }
    }
}
