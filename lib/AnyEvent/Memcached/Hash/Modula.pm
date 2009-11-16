package AnyEvent::Memcached::Hash::Modula;
use Any::Moose;
use String::CRC32 qw(crc32);
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Hash';

sub hash {
    my ($self, $key) = @_;
    return (crc32($key) >> 16) & 0x7fff;
}

__PACKAGE__->meta->make_immutable();

1;
