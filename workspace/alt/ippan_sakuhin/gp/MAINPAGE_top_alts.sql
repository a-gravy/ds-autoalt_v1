select
    feature_public_code,
    unnest(string_to_array(sakuhin_codes,'|')) as sakuhin_public_code
from dim_feature
where feature_public_code = 'FET0001835' or feature_public_code = 'FET0001836'