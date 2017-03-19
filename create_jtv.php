<?PHP

date_default_timezone_set('Europe/Minsk');

Define('FB_HOST','127.0.0.1'); 
Define('FB_DB',  'A4on_DB');
Define('FB_USER','SYSDBA');
Define('FB_PASS','masterkey');

Define('TMP_DIR', __DIR__.'/');
Define('JTV_NAME','epg.zip');

function get_filename($str) {
    $result = iconv("UTF-8", "windows-1251", $str);
    $f = array('+',' ','.', "'", '"');
    $r = array_fill(0, count($f), '_');
    return str_replace($f, $r, $result);
}

if (!file_exists(TMP_DIR))
    mkdir(TMP_DIR, 777, true);

$db = new fbsql_db(FB_HOST, FB_DB, FB_USER, FB_PASS);


$sql = "select
  c.Ch_Id ID,
  c.CH_NAME as NAME,
  (select count(*) from epg e where e.Ch_Id = c.Ch_Id and e.Epg_Date >= current_date) CNT
from Channels c
order by 1";
$chennals = $db->select_assoc_all($sql);

$create_zip = true;
$jtv_zip = new ZipArchive();
if ($jtv_zip->open(TMP_DIR.JTV_NAME, ZipArchive::CREATE) != true)
    $create_zip = false;

foreach($chennals as $ch) {
    
    if ($ch['CNT'] == 0) continue;
    
    $filename = get_filename($ch['NAME']);
    
    $sql = "select
                coalesce(DATE_START, '')   DATE_START,
                coalesce(TITLE, '')        TITLE,
                coalesce(DESCRIPTION, '')  DESCRIPTION,
                coalesce(GENRES, '')       GENRES, 
                coalesce(MINAGE, '')       MINAGE,
                coalesce(CREATE_YEAR, '')  CREATE_YEAR,
                coalesce(ACTORS, '')       ACTORS, 
                coalesce(DIRECTED, '')     DIRECTED, 
                coalesce(COUNTRY, '')      COUNTRY
            from epg e where e.Ch_Id = {$ch['ID']} and e.Epg_Date >= current_date
            order by 1";
    $epg = $db->select_assoc_all($sql);
    
    $count = count($epg);
    
    $ndx_filename = "$filename.ndx";
    $ndx = fopen(TMP_DIR.$ndx_filename, "wb");
    fwrite($ndx, pack("v", $count), 2); // количество телепередач (2 байт)
    
    $pdt_filename = "$filename.pdt";
    $pdt = fopen(TMP_DIR.$pdt_filename, "wb");
    fwrite($pdt, "JTV 3.x TV Program Data\x0A\x0A\x0A", 26); // заголовок файла (26 байт)
    $offset = 26;
    
    foreach ($epg as $prg) {
        $title  = $prg['TITLE'].'.';
        if ($prg['MINAGE'] <> '') $title .= ' '.$prg['MINAGE'].'+';
        if ($prg['GENRES'] <> '') $title .= ' '.$prg['GENRES'].'.';
        if ($prg['COUNTRY'] <> '') $title .= ' '.$prg['COUNTRY'].'.';
        if ($prg['CREATE_YEAR'] <> '') $title .= ' '.$prg['CREATE_YEAR'].'.';
        if ($prg['DESCRIPTION'] <> '') $title .= ' '.$prg['DESCRIPTION'].'.';
        if ($prg['DIRECTED'] <> '') $title .= ' '.$prg['DIRECTED'].'.';
        if ($prg['ACTORS'] <> '') $title .= ' '.$prg['ACTORS'].'.';
        $programme = @iconv("UTF-8", "windows-1251", $title);
        if (strlen($programme) > 256) $title = substr($programme, 0, 256);
        
        $len = strlen($programme);
        fwrite($pdt, pack("v", $len), 2); // длина названия телепередачи (2 байт)
        fwrite($pdt, $programme, $len);   // название телепередачи
        $offset += $len + 2;
        
        $timestamp = strtotime($prg['DATE_START']);
        
        fwrite($ndx, "\x00\x00", 2); // заголовок записи (2 байт)
        $filetime = bcmul(bcadd($timestamp, "11644473600"), "10000000"); // Win32 FILETIME
        for ($i = 0; $i < 8; $i++) { // дата и время начала телепередачи (8 байт)
            $byte = (int) bcmod($filetime, "256");
            fwrite($ndx, pack("c", $byte), 1);
            $filetime = bcdiv($filetime, "256");
        }
        fwrite($ndx, pack("v", $offset), 2); // смещение - указатель на длину названия телепередачи в файле .pdt (2 байт)
    }
    
    // Сбросили буфера
    fflush ($pdt);
    fflush ($ndx);
    // Закрыли файлы
    fclose($ndx);
    fclose($pdt);
  
    if ($create_zip) {
        $jtv_zip->addFile(TMP_DIR.$ndx_filename, iconv("windows-1251", "CP866", $ndx_filename));
        $jtv_zip->addFile(TMP_DIR.$pdt_filename, iconv("windows-1251", "CP866", $pdt_filename));
    }
}

if ($create_zip)
    $archive_closed = $jtv_zip->close();

class fbsql_db {

    var $session;
    var $debug = false;
    
    // Конструктор класса сервер, база данных, пользователь, пароль
    function fbsql_db($sqlserver, $database, $sqluser='SYSDBA', $sqlpassword='masterkey', $charset = 'UTF8', $persistency = true) {
        
        $this->user     = $sqluser;
        $this->password = $sqlpassword;
        $this->server   = $sqlserver;
        $this->dbname   = $database;
        
        if($persistency) {
            $this->session = ibase_pconnect($this->server.":".$this->dbname, $this->user, $this->password, $charset);
        }
        else {
            $this->session = ibase_connect($this->server.":".$this->dbname, $this->user, $this->password, $charset);
        }
        
        return $this->session;
    }
    
    // удаление объекта
    function __destruct() {
        if($this->session)
        {
            $result = @ibase_close($this->session);
            return $result;
        }
        else
        {
            return false;
        }
    }
    
    // Выборка всего результата в ассоциативный массив
    function select_assoc_all($sql = "") {
        if($sql != "") {
            if ($this->debug) { echo $sql.PHP_EOL;}
            $transaction = ibase_trans(IBASE_READ + IBASE_NOWAIT, $this->session);
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
        }
        else {
            return FALSE;
        }
    }
    
    function close() {
        if($this->session) {
            $result = @ibase_close($this->session);
            return $result;
        }
        else {
            return false;
        }
    }    
} 