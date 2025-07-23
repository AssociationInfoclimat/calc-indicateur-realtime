<?php

namespace Infoclimat\Cron\Comephore\CalcIndicateurRealtime\Main;

require_once __DIR__ . '/calc_indicateur_realtime.php';

use Exception;
use Infoclimat\Cron\Comephore\CalcIndicateurRealtime\InMemoryIndicateurPluvioRepository;
use Infoclimat\Cron\Comephore\CalcIndicateurRealtime\MySQLIndicateurPluvioRepository;

use function Infoclimat\Cron\Comephore\CalcIndicateurRealtime\calc_indicateur;

/**
 * @throws Exception
 */
function execute(): void
{
    $end = date('Y-m-d');
    $start = date('Y-m-d', time() - 3 * 24 * 3600);
    $indicateur_pluvio_repository = getenv('APP_ENV') === 'production'
        ? new MySQLIndicateurPluvioRepository()
        : new InMemoryIndicateurPluvioRepository();
    calc_indicateur($start, $end, $indicateur_pluvio_repository);
}

execute();
