import os
from pipes.ghdh.sex import ghdh_sex_pour
from pipes.gho.oop import gho_oop_pour
from pipes.hello_world.hello import hello_pour
from pipes.iaho.iaho import iaho_pour
from pipes.idsr.idsr import idsr_pour
from pipes.tracss.tracss import tracss_pour
from pipes.ucndw.vaccine_coverage import vaccine_coverage_pour
from pipes.util.load_dotenv import load_local_dotenv
from pipes.util.logger import log
from pipes.wmr.wmr import wmr_pour


def main():
    load_local_dotenv()
    log.setLevel(os.getenv("LOG_LEVEL", "DEBUG"))

    # test and execute pipelines
    hello_pour.test()
    iaho_pour.test()  # DECOMISSIONED: delete after 2025-01-01
    vaccine_coverage_pour.test()
    tracss_pour.test()
    wmr_pour.test()
    idsr_pour.test()
    ghdh_sex_pour.test()
    gho_oop_pour.test()


if __name__ == "__main__":
    main()
