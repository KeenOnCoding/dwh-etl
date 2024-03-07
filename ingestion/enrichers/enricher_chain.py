from operator import itemgetter
from ingestion.enrichers.abstract_enricher import AbstractEnricher
from commons.logging.simple_logger import logger

class EnricherChain:
    """
    EnricherChain allows to set different enrichers with
    corresponding enricher priority and apply all the
    transformations at once
    """
    __slots__ = ('chain',)

    def __init__(self):
        self.chain = list()

    def add_enricher(self, enricher: AbstractEnricher, priority):
        self.chain.append((priority, enricher))
        # sort by first item - priority
        self.chain.sort(key=itemgetter(0))
        logger.info(f"Added {type(enricher).__name__} enricher to enrichment chain with {priority} priority.")

    def enrich(self, df, context):
        output = df

        if output.count() == 0:
            logger.info("DataFrame is empty")
            return output

        for chain_item in self.chain:
            enricher = chain_item[1]
            output = enricher.enrich(output, context)

        return output
