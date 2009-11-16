use inc::Module::Install;

name 'AnyEvent-Memcached';
requires 'Any::Moose';
requires 'AnyEvent';
requires 'AnyEvent::Handle';
requires 'AnyEvent::Socket';
requires 'namespace::clean';

WriteAll;