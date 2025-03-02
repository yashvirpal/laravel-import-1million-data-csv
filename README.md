

# Import One Million Rows To The Database (Laravel/PHP)


This repo includes:

- Examples of CSV files from Git LFS
- A `CustomersImportCommand` console command that will automatically show benchmarks for the current command
- An `ImportHelper` trait that contains the code for benchmarking and several approaches I show in the video

Please be aware that this video and code is about having fun trying to import huge CSV files. Many real-world aspects, like validation, data quality, multiple tables, etc., are not included.



### Logging & Events

Eloquent logs queries and throws events, which can be heavy, too. You can disable that by using:

```php
DB::disableQueryLog();
DB::connection()->unsetEventDispatcher();
```

If you disable them for the example `05` and `06`, you will see that we can also import 1M rows.
