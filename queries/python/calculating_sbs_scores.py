from suricate.sbstransformers import SbsApplyComparator
from sklearn.pipeline import FeatureUnion
_sbs_score_list = [
    ('firstnames_fuzzy', SbsApplyComparator(on='firstnames', comparator='simple')),
    ('firstnames_token', SbsApplyComparator(on='firstnames', comparator='token')),
    ('surname_fuzzy', SbsApplyComparator(on='surname', comparator='simple')),
    ('surname_token', SbsApplyComparator(on='surname', comparator='token')),
    ('route_fuzzy', SbsApplyComparator(on='route', comparator='simple')),
    ('route_token', SbsApplyComparator(on='route', comparator='token')),
    ('locality_fuzzy', SbsApplyComparator(on='locality', comparator='simple')),
    ('locality_token', SbsApplyComparator(on='locality', comparator='token'))
]
import pandas as pd
import psycopg2
def pg_hook():
    return psycopg2.connect(dbname="mydb", user="myuser", password="mypassword", host='localhost', port=5439)

conn = pg_hook()
conn.autocommit = True
df = pd.read_sql("""SELECT * FROM mlm.suricate_sbs""", conn).set_index(['ix_source', 'ix_target'], drop=True)

scorer_sbs  = FeatureUnion(transformer_list=_sbs_score_list)
X_scores = scorer_sbs.fit_transform(X=df)
X_scores = pd.DataFrame(X_scores, index=df.index,  columns=[c[0] for c in _sbs_score_list])
X_scores.to_csv('/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/sbs_scores.csv', index=True, encoding='utf-8', sep=',')

