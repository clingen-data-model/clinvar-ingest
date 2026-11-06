"""
Keep vrsify's translation off the network services vrs-python and hgvs can reach on
their own: UTA (the biocommons transcript database) and NCBI sequence fetching. vrsify
translates genomic expressions against a RefgetStore, so a record that would need either
service gets an error rather than a live request.

vrs-python's HgvsTools connects to UTA when it is created, to build an hgvs normalizer and
variant mapper; the genomic path uses neither. Replacing `hgvs.dataproviders.uta.connect`
with a placeholder data provider keeps that construction from connecting, and any use of
the placeholder raises. hgvs's SeqFetcher falls back to `bioutils.seqfetcher.fetch_seq`,
which fetches from NCBI, and binds it when constructed, so these must be disabled before
any translator is created.
"""

import bioutils.seqfetcher
import hgvs.dataproviders.uta


class RemoteServiceDisabledError(RuntimeError):
    pass


class DisabledUTA:
    """A data provider in place of a UTA connection: closing it is allowed, using it raises."""

    def close(self) -> None:
        pass

    def __getattr__(self, name: str):
        raise RemoteServiceDisabledError(f"UTA access is disabled in vrsify (attempted {name})")


def _disabled_uta_connect(*_args, **_kwargs) -> DisabledUTA:
    return DisabledUTA()


def _disabled_fetch_seq(ac: str, *_args, **_kwargs):
    raise RemoteServiceDisabledError(f"network sequence fetching is disabled in vrsify (attempted {ac})")


def disable_remote_services() -> None:
    hgvs.dataproviders.uta.connect = _disabled_uta_connect
    bioutils.seqfetcher.fetch_seq = _disabled_fetch_seq
