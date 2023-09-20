mod sle {
    use stool::sle::{Engine, Operation, Rule, Statement};

    #[test]
    fn serde() {
        let engine = engine();
        let data =
            ron::ser::to_string_pretty(&engine, ron::ser::PrettyConfig::new().struct_names(true))
                .unwrap();
        assert_eq!(
            ron::from_str::<Engine>(&data).unwrap(),
            engine,
            "round-trip works"
        );
    }

    #[test]
    fn matching() {
        let engine = engine();
        let csv = fixture("sponsors-2021.csv");
        let mut num_matches = 0;
        for record in csv.into_byte_records() {
            let record = record.unwrap();
            if let Some(rule) = engine.matching_rule(&record) {
                num_matches += 1;
                assert_eq!(&record[0], b"Oneitho");
                assert_eq!(rule.value, "the annotation");
            }
        }

        assert_eq!(
            num_matches, 1,
            "exactly one row matches the rule configuration"
        );
    }

    fn fixture(name: &str) -> csv::Reader<std::fs::File> {
        csv::Reader::from_path(std::path::Path::new("tests").join("fixtures").join(name)).unwrap()
    }

    fn engine() -> Engine {
        Engine {
            rules: vec![Rule {
                statements: vec![
                    Statement {
                        value_column_index: 0,
                        operation: Operation::Equals,
                        value: "Oneitho".into(),
                    },
                    Statement {
                        value_column_index: 8,
                        operation: Operation::EndsWith,
                        value: "one time".into(),
                    },
                ],
                value: "the annotation".to_string(),
            }],
        }
    }
}

#[test]
fn normalize_number() {
    for (input, expected) in [
        ("$10.00", "$10,00"),
        ("$1,000.00", "$1.000,00"),
        ("$1,000", "$1.000"),
        ("€8,75", "€8,75"),
        ("€1,000,00", "€1.000,00"),
        ("€1,000,000,00", "€1.000.000,00"),
        ("$1,000,000.00", "$1.000.000,00"),
    ] {
        let actual = stool::normalize_number(input, '.', ',');
        assert_eq!(
            actual,
            expected.as_bytes(),
            "{} != {expected}",
            std::str::from_utf8(&actual).unwrap()
        );
    }
}
