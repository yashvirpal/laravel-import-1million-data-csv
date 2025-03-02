<?php

namespace App\Console\Commands;

use App\Traits\ImportHelper;
use Illuminate\Console\Command;

class CustomersImportCommand extends Command
{
    // Rules:
    // 1. MySQL
    // 2. 256MB Memory Limit
    // 3. No Queue
    use ImportHelper;

    protected $signature = 'import:customers';

    protected $description = 'Import customers from CSV file.';

    public function handleImport($filePath): void
    {
        // Have fun running imports here
        // Examples can be found in the ImportHelper trait
    }
}
