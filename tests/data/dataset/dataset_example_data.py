# encoding: utf-8

complete_dataset = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender", "measure"],
    "value": [1,2,3,4],
    "status": { 1: "x" },
    "size": [2,2,1],
    "updated": "2016-10-10",
    "label": u"Test dataset with åäö",
    "source": "My source",
    "note": [ "My dataset note" ],
    "dimension": {
        "region": {
            "category": {
                "index": {
                    "Solna": 1,
                    "Stockholm": 0,
                },
                "label": {
                    "Solna": "Solna kommun",
                    "Stockholm": "Stockholm kommun",
                },
                "note": {
                    "Solna": [ "My Solna note" ]
                }
            },
            "note": [ "My region note" ],
            "label": "Region",
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            },
            "label": u"Kön",
        },
        "measure": {
            "category": {
                "index": ["share"],
                "label": {
                    "share": "Antal"
                },
                "unit": {
                    "share": {
                        "decimals:": 1,
                        "label": "%"
                    }
                }
            }
        }
    },
    "extension": {
        "description": "An example description"
    }
}

dataset_to_append = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender", "measure"],
    "value": [5,6],
    "size": [1,2,1],
    "label": "Test dataset",
    "source": "My source",
    "dimension": {
        "region": {
            "category": {
                "index": {
                    u"Malmö": 0,
                },
                "label": {
                    u"Malmö": u"Malmö kommun",
                }
            },
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            },
        },
        "measure": {
            "category": {
                "index": ["share"],
                "label": {
                    "share": "Andel"
                },
                "unit": {
                    "share": {
                        "decimals:": 1,
                        "label": "%"
                    }
                }
            }
        }
    }
}

dataset_to_append_with_overlap = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender", "measure"],
    "value": [50,60],
    "size": [1,2,1],
    "label": "Test dataset",
    "source": "My source",
    "note": [ "This is a conflicting dataset note" ],
    "dimension": {
        "region": {
            "category": {
                "index": {
                    u"Solna": 0,
                },
                "label": {
                    u"Solna": u"Solna kommun",
                },
                "note": {
                    u"Solna": [ "Conflicting Solna note" ],
                }
            },
            "note": [ "This is conflicting note" ]
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            },
        },
        "measure": {
            "category": {
                "index": ["share"],
                "label": {
                    "share": "Antal"
                },
                "unit": {
                    "share": {
                        "decimals:": 1,
                        "label": "%"
                    }
                }
            }
        }
    }
}

# EXAMPLES OF INCOMPLETE DATA

missing_dimension_dataset = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender", "foo"], # Foo not in dimension
    "value": [1,2],
    "size": [1,2],
    "dimension": {
        "region": {
            "category": {
                "index": {
                    "Stockholm": 0
                }
            }
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            }
        }
    }
}
missing_id_dataset = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region"], # Dimension missing!
    "value": [1,2],
    "size": [],
    "dimension": {
        "region": {
            "category": {
                "index": {
                    "Stockholm": 0
                }
            }
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            }
        }
    }
}

wrong_size_dataset = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender"],
    "value": [1,2],
    "size": [1,1], # Error here!
    "dimension": {
        "region": {
            "category": {
                "index": {
                    "Stockholm": 0
                }
            }
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            }
        }
    }
}

wrong_value_length_dataset = {
    "version": "2.0",
    "class": "dataset",
    "id": ["region", u"gender"],
    "value": [1],
    "size": [1,1], # Error here!
    "dimension": {
        "region": {
            "category": {
                "index": {
                    "Stockholm": 0
                }
            }
        },
        "gender": {
            "category": {
                "index": [ "M", "F" ]
            }
        }
    }
}
