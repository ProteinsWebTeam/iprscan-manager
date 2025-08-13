include { INIT_PIPELINE    } from "./init"
include { IMPORT_SEQUENCES } from "../../modules/sequences"

workflow IMPORT {
    take:
    database_params
    top_up
    max_upi

    main:
    INIT_PIPELINE(
        database_params,
        top_up
    )
    db_config = INIT_PIPELINE.out.dbConfig.val

    IMPORT_SEQUENCES(
        db_config["intprscan"],
        db_config["uniparc"],
        top_up,
        max_upi
    )
}
