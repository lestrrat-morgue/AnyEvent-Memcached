package AnyEvent::Memcached::Hash;
use Moose;
use namespace::clean -except => qw(meta);

sub hash { confess "implement hash" }

__PACKAGE__->meta->make_immutable();

1;

