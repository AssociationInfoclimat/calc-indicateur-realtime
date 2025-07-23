<?php

namespace Infoclimat\Cron\Comephore\CalcIndicateurRealtime\Test;

require_once __DIR__ . '/calc_indicateur_realtime.php';

use Exception;
use Infoclimat\Cron\Comephore\CalcIndicateurRealtime\InMemoryIndicateurPluvioRepository;

use function Infoclimat\Cron\Comephore\CalcIndicateurRealtime\calc_indicateur;

/**
 * @throws Exception
 */
function execute(): void
{
    $start = '2018-12-31';
    $end = '2019-01-02';
    $indicateur_pluvio_repository = new InMemoryIndicateurPluvioRepository();
    calc_indicateur($start, $end, $indicateur_pluvio_repository);
    var_dump($indicateur_pluvio_repository);
}

execute();
