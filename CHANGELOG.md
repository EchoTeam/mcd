## Version 1.1.0

    1. Fixed 'hot code reload' bug. Got rid of lambda-function in 'mcd.erl'.
    2. dht_ring' was moved to deps.
    3. Added specs to all external (and not BC) functions in 'mcd.erl'.
    4. Added well-named functions to all operations. Old-style function
       do' was marked as prohibited, will be removed in next version.
    5. Added function for emulation "overload" error for tests purposes.
    6. All usages of deprecated method 'delete/2' were replaced with
       'delete/1'.
    7. mcd_cluster main logic was reworked.
    8. mcd_starter replaced to mcd_cluster_sup and made more stable.
    9. Added mcd_parallel to work with several buckets synchronously.
    10. Added a lot of tests.
    11. Marked old convention about 'localmcd' as prohibited (and will be
        deleted in the future releases).
    12. Renamed 'install' target in Makefile to 'install-lib'. Added some
        new targets.
