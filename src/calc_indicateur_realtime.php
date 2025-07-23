<?php

namespace Infoclimat\Cron\Comephore\CalcIndicateurRealtime;

use Exception;
use PDO;
use PDOStatement;
use Throwable;

/**
 * @throws Exception
 */
function load_pdo_ip(): string|array
{
    $ip = getenv('DB_HOST');
    if (empty($ip)) {
        throw new Exception('Missing DB_HOST in environment variables');
    }
    return $ip;
}

/**
 * @throws Exception
 */
function load_pdo_username(): string|array
{
    $username = getenv('DB_USER');
    if (empty($username)) {
        throw new Exception('Missing DB_USER in environment variables');
    }
    return $username;
}

/**
 * @throws Exception
 */
function load_pdo_password(): string|array
{
    $password = getenv('DB_PASSWORD');
    if (empty($password)) {
        throw new Exception('Missing DB_PASSWORD in environment variables');
    }
    return $password;
}

/**
 * @throws Exception
 */
function load_pdo_config(): array
{
    $ip = load_pdo_ip();
    $username = load_pdo_username();
    $password = load_pdo_password();
    return [$ip, $username, $password];
}

/**
 * @throws Exception
 */
function connexion_sql(string $db): PDO
{
    [$ip, $username, $password] = load_pdo_config();
    return new PDO(
        "mysql:host={$ip};dbname={$db}",
        $username,
        $password
    );
}

interface IndicateurPluvioRepository
{
    public function prepare(string $sql): void;

    public function execute(array $data): void;
}

class MySQLIndicateurPluvioRepository implements IndicateurPluvioRepository
{
    private PDO           $pdo;
    private ?PDOStatement $stmt = null;

    /**
     * @throws Exception
     */
    public function __construct()
    {
        $this->pdo = connexion_sql('V5_climato');
    }

    public function prepare(string $sql): void
    {
        $this->stmt = $this->pdo->prepare($sql);
    }

    /**
     * @throws Exception
     */
    public function execute(array $data): void
    {
        if ($this->stmt === null) {
            throw new Exception('Statement not prepared');
        }
        $this->stmt->execute($data);
    }
}

class InMemoryIndicateurPluvioRepository implements IndicateurPluvioRepository
{
    public string $preparedSql  = '';
    public array  $executedRows = [];

    public function prepare(string $sql): void
    {
        $this->preparedSql = $sql;
    }

    public function execute(array $data): void
    {
        $this->executedRows[] = $data;
    }
}

/**
 * @throws Exception
 */
function calc_indicateur(
    string                     $start,
    string                     $end,
    IndicateurPluvioRepository $indicateur_pluvio_repository
): void {
    $data_output = [];
    $fields_names = [];
    $DIR = __DIR__;
    exec("cd {$DIR}/.. && poetry run python ./calcul_indicateur_rr/calcul_indicateur_RR.py {$start} {$end}", $data_output);

    foreach ($data_output as $line_index => $line) {
        if ($line_index == 0) {
            $fields_names = explode(',', $line);
            break;
        }
    }

    $fields_placeholders = implode(', ', array_map(fn($name) => ":{$name}", $fields_names));
    $fields_names_sql = implode(', ', $fields_names);
    $indicateur_pluvio_repository->prepare(
        <<<SQL
            INSERT INTO V5_climato.indicateur_pluvio({$fields_names_sql})
            VALUES                                  ({$fields_placeholders})
            SQL
    );

    foreach ($data_output as $line_index => $line) {
        if ($line_index == 0) {
            continue;
        }
        if (trim($line) == ',' || trim($line) == '') {
            continue;
        }

        $line = explode(',', $line);
        $data = [];
        foreach ($fields_names as $field_index => $field_name) {
            $data[$field_name] = $line[$field_index];
        }

        try {
            $indicateur_pluvio_repository->execute($data);
        } catch (Throwable $th) {
            echo $th;
        }
    }
}
