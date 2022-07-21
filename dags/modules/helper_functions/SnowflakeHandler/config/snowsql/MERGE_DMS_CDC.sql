MERGE into {} using {} on {}
    when matched and {}."Op"='D' then delete
    when matched and {}."Op"='U' then update set {}
    when not matched and {}."Op"='I' then insert {} values {};