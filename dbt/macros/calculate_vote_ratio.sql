{% macro calculate_vote_ratio(numerator, denominator) %}
    case 
        when {{ denominator }} = 0 then 0 
        else round(({{ numerator }}::numeric / {{ denominator }}::numeric) * 100, 2)
    end
{% endmacro %}
