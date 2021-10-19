#!/usr/bin/env python3

import config as cfg


def read_config_names():
    config_names = []
    # We fill the configs from all of the possible classes in config.py so that if we add a new config,
    # we don't need to add it here. And this avoids the problem where we forget to add it here
    for x in cfg.__dict__.values():
        if cfg.should_include_config(x):
            config_names.append(x.__name__)
    return config_names


def print_config_names():
    config_names = read_config_names()
    for config_name in config_names:
        print(config_name)


if __name__ == "__main__":
    print_config_names()
